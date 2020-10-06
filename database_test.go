package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rnovales/teleport/schema"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.starlark.net/starlark"
)

var (
	widgetsTableDefinition = readTableFromConfigFile("testdata/example_widgets.yml")
)

func TestDatabaseConfigurationCases(t *testing.T) {
	cases := []struct {
		testFile                string
		table                   string
		expectedLastEntryLevel  log.Level
		expectedRows            int
		expectLastEntryContains []string
	}{
		{
			"full.port",
			"widgets",
			log.InfoLevel,
			10,
			[]string{},
		},
		{
			"default.port",
			"widgets",
			log.InfoLevel,
			10,
			[]string{},
		},
		{
			"modified_only.port",
			"objects",
			log.InfoLevel,
			2,
			[]string{},
		},
		{
			"missing.port",
			"objects",
			log.InfoLevel,
			3,
			[]string{},
		},
		{
			"star.port",
			"objects",
			log.InfoLevel,
			2,
			[]string{},
		},
	}

	for _, cse := range cases {
		t.Run(cse.testFile, func(t *testing.T) {
			runDatabaseTest(t, cse.testFile, func(t *testing.T, portFile string, dbSrc *schema.Database, dbDest *schema.Database) {
				setupTable(dbSrc, cse.table)

				extractLoadDatabase(portFile, "testdest", cse.table)

				assert.Equal(t, cse.expectedLastEntryLevel, hook.LastEntry().Level)
				if cse.expectedRows != -1 {
					assertRowCount(t, cse.expectedRows, dbDest, fmt.Sprintf("testsrc_%s", cse.table))
				}
				for _, contains := range cse.expectLastEntryContains {
					lastString, err := hook.LastEntry().String()
					assert.NoError(t, err)
					assert.Contains(t, lastString, contains)
				}
			})
		})
	}
}

func TestColumnTransforms(t *testing.T) {
	cases := []struct {
		testFile                       string
		table                          string
		transformedColumnName          string
		transformedColumnFirstRowValue interface{}
		transformedColumnType          schema.DataType
	}{
		{
			"transform_column.port",
			"widgets",
			"ranking",
			62.19171468260465,
			schema.FLOAT,
		},
		{
			"transform_column_and_change_type.port",
			"widgets",
			"ranking",
			int64(62),
			schema.INTEGER,
		},
	}

	for _, cse := range cases {
		t.Run(cse.testFile, func(t *testing.T) {
			runDatabaseTest(t, cse.testFile, func(t *testing.T, portFile string, dbSrc *schema.Database, dbDest *schema.Database) {
				setupTable(dbSrc, cse.table)

				extractLoadDatabase(portFile, "testdest", cse.table)

				if hook.LastEntry().Level != log.InfoLevel {
					assert.FailNow(t, "unexpect log level encountered")
				}
				var value interface{}
				err := dbDest.QueryRow(fmt.Sprintf("SELECT %s FROM testsrc_%s LIMIT 1", cse.transformedColumnName, cse.table)).Scan(&value)
				assert.NoError(t, err)
				assert.Equal(t, cse.transformedColumnFirstRowValue, value)

				table, err := dbDest.DumpTableMetadata(fmt.Sprintf("testsrc_%s", cse.table))
				assert.NoError(t, err)
				for _, column := range table.Columns {
					if column.Name == cse.transformedColumnName {
						assert.Equal(t, cse.transformedColumnType, table.Columns[2].DataType)
					}
				}
			})
		})
	}
}

func TestComputedColumns(t *testing.T) {
	cases := []struct {
		testFile                    string
		table                       string
		optionsColumnValue          string
		computedColumnName          string
		computedColumnIndex         int
		computedColumnFirstRowValue string
		computedColumnType          schema.DataType
	}{
		{
			"compute_column.port",
			"widgets",
			"",
			"created_date",
			8,
			"2020-03-11",
			schema.DATE,
		},
		{
			"deserialize_json_column.port",
			"actions",
			"{\"time_zone\":\"Mountain Time (US & Canada)\"}",
			"time_zone",
			3,
			"Mountain Time (US & Canada)",
			schema.TEXT,
		},
		{
			"deserialize_ruby_yaml_column.port",
			"actions",
			"--- !ruby/hash-with-ivars:ActionController::Parameters\nelements:\n  append: 'Hello!'\n  prepend: ''\n  custom_message_text: ''\n  click_tracking: &1 []\nivars:\n  :@permitted: false\n  :@converted_arrays: !ruby/object:Set\n    hash:\n      *1: true\n",
			"append",
			3,
			"Hello!",
			schema.TEXT,
		},
	}

	for _, cse := range cases {
		t.Run(cse.testFile, func(t *testing.T) {
			runDatabaseTest(t, cse.testFile, func(t *testing.T, portFile string, dbSrc *schema.Database, dbDest *schema.Database) {
				setupTable(dbSrc, cse.table)
				if cse.optionsColumnValue != "" {
					actionsInsert(dbSrc, 1, "Joe", cse.optionsColumnValue)
				}

				extractDatabase(portFile, cse.table)

				csvfile := hook.LastEntry().Data["file"].(string)
				assertCsvCellContents(t, cse.computedColumnFirstRowValue, csvfile, 1, cse.computedColumnIndex, "`%s` column value not equal", cse.computedColumnName)

				var table schema.Table
				var tableExtract TableExtract
				readTableExtractConfiguration(portFile, cse.table, &tableExtract)
				inspectTable("testsrc", cse.table, &table, &tableExtract)
				assert.Equal(t, cse.computedColumnType, table.Columns[cse.computedColumnIndex].DataType)
			})
		})
	}
}

func TestComputedColumnsIncludedWhenCreatingTable(t *testing.T) {
	runDatabaseTest(t, "compute_column.port", func(t *testing.T, portFile string, dbSrc *schema.Database, dbDest *schema.Database) {
		setupTable(dbSrc, "widgets")

		extractLoadDatabase(portFile, "testdest", "widgets") // Destination table does not exist, so will be created

		assert.Equal(t, log.InfoLevel, hook.LastEntry().Level)

		var createdDate string
		row := dbDest.QueryRow("SELECT CAST(created_date AS VARCHAR(32)) FROM testsrc_widgets ORDER BY id LIMIT 1")
		err := row.Scan(&createdDate)
		assert.NoError(t, err)

		assert.Equal(t, "2020-03-11", createdDate)
	})
}

func TestFullLoadFlag(t *testing.T) {
	FullLoad = true
	var (
		testFile               string    = "modified_only.port"
		table                  string    = "objects"
		expectedLastEntryLevel log.Level = log.InfoLevel
		expectedRows           int       = 3 // ModifiedOnly strategy expects only 2 rows, 3 signifies a Full load
	)

	runDatabaseTest(t, testFile, func(t *testing.T, portFile string, dbSrc *schema.Database, dbDest *schema.Database) {
		setupTable(dbSrc, table)

		extractLoadDatabase(portFile, "testdest", table)

		assert.Equal(t, expectedLastEntryLevel, hook.LastEntry().Level)
		if expectedRows != -1 {
			assertRowCount(t, expectedRows, dbDest, fmt.Sprintf("testsrc_%s", table))
		}
	})
	FullLoad = false
}

// Skip this test until schema for SQLite staging tables is fixed
func xTestSQLiteLoadExtractLoadConsistency(t *testing.T) {
	runDatabaseTest(t, "full.port", func(t *testing.T, _ string, srcdb *schema.Database, destdb *schema.Database) {
		srcdb.Exec(srcdb.GenerateCreateTableStatement("widgets", widgetsTableDefinition))

		err := importCSV("testsrc", "widgets", "testdata/example_widgets.csv", widgetsTableDefinition.Columns)
		assert.NoError(t, err)
		extractLoadDatabase("testsrc", "testdest", "widgets")

		newTable, err := destdb.DumpTableMetadata("testsrc_widgets")
		assert.NoError(t, err)
		assertRowCount(t, 10, destdb, "testsrc_widgets")
		assert.Equal(t, widgetsTableDefinition.Columns, newTable.Columns)
	})
}

func TestLoadSourceHasAdditionalColumn(t *testing.T) {
	runDatabaseTest(t, "full.port", func(t *testing.T, _ string, srcdb *schema.Database, destdb *schema.Database) {
		// Create a new schema.Table Definition, same as widgets, but without the `description` column
		widgetsWithoutDescription := schema.Table{"example", "widgets", make([]schema.Column, 0)}
		widgetsWithoutDescription.Columns = append(widgetsWithoutDescription.Columns, widgetsTableDefinition.Columns[:2]...)
		widgetsWithoutDescription.Columns = append(widgetsWithoutDescription.Columns, widgetsTableDefinition.Columns[3:]...)

		srcdb.Exec(srcdb.GenerateCreateTableStatement("widgets", widgetsTableDefinition))
		destdb.Exec(destdb.GenerateCreateTableStatement("testsrc_widgets", &widgetsWithoutDescription))
		importCSV("testsrc", "widgets", "testdata/example_widgets.csv", widgetsTableDefinition.Columns)

		expectLogMessages(t, []string{"destination table does not define column", "ranking"}, func() {
			extractLoadDatabase("testsrc", "testdest", "widgets")
		})

		assertRowCount(t, 10, destdb, "testsrc_widgets")
	})
}

func TestLoadStringNotLongEnough(t *testing.T) {
	runDatabaseTest(t, "full.port", func(t *testing.T, _ string, srcdb *schema.Database, destdb *schema.Database) {
		// Create a new schema.Table Definition, same as widgets, but with name LENGTH changed to 32
		widgetsWithShortName := schema.Table{"example", "widgets", make([]schema.Column, len(widgetsTableDefinition.Columns))}
		copy(widgetsWithShortName.Columns, widgetsTableDefinition.Columns)
		widgetsWithShortName.Columns[3] = schema.Column{"name", schema.STRING, map[schema.Option]int{schema.LENGTH: 32}}

		srcdb.Exec(srcdb.GenerateCreateTableStatement("widgets", widgetsTableDefinition))
		destdb.Exec(destdb.GenerateCreateTableStatement("testsrc_widgets", &widgetsWithShortName))

		expectLogMessage(t, "For string column `name`, destination LENGTH is too short", func() {
			extractLoadDatabase("testsrc", "testdest", "widgets")
		})
	})
}

func TestExportTimestamp(t *testing.T) {
	runDatabaseTest(t, "full.port", func(t *testing.T, _ string, srcdb *schema.Database, _ *schema.Database) {
		columns := make([]schema.Column, 0)
		columns = append(columns, schema.Column{"created_at", schema.TIMESTAMP, map[schema.Option]int{}})
		table := schema.Table{"test1", "timestamps", columns}

		srcdb.Exec(srcdb.GenerateCreateTableStatement("timestamps", &table))
		srcdb.Exec("INSERT INTO timestamps (created_at) VALUES (DATETIME(1092941466, 'unixepoch'))")
		srcdb.Exec("INSERT INTO timestamps (created_at) VALUES (NULL)")

		currentWorkflow = &Workflow{Thread: &starlark.Thread{}}
		tempfile, _ := exportCSV("testsrc", "timestamps", columns, "", TableExtract{}, false)

		assertCsvCellContents(t, "2004-08-19T18:51:06Z", tempfile, 0, 0)
		assertCsvCellContents(t, "", tempfile, 1, 0)
	})
}

func TestDatabasePreview(t *testing.T) {
	Preview = true
	runDatabaseTest(t, "full.port", func(t *testing.T, _ string, srcdb *schema.Database, _ *schema.Database) {
		setupObjectsTable(srcdb)

		expectLogMessage(t, "(not executed)", func() {
			extractLoadDatabase("testsrc", "testdest", "objects")
		})

		actual, _ := tableExists("testdest", "testsrc_objects")
		assert.False(t, actual)
	})
	Preview = false
}

func runDatabaseTest(t *testing.T, testfile string, testfn func(*testing.T, string, *schema.Database, *schema.Database)) {
	withTestDatabasePortFile(t, "testsrc", testfile, func(t *testing.T, filename string) {
		Databases["testsrc"] = Database{"sqlite://:memory:", map[string]string{}, true}
		dbSrc, err := connectDatabase("testsrc")
		if err != nil {
			assert.FailNow(t, "%w", err)
		}
		defer delete(dbs, "testsrc")

		Databases["testdest"] = Database{"sqlite://:memory:", map[string]string{}, false}
		dbDest, err := connectDatabase("testdest")
		if err != nil {
			assert.FailNow(t, "%w", err)
		}
		defer delete(dbs, "testdest")

		redirectLogs(t, func() {
			testfn(t, filename, dbSrc, dbDest)
		})
	})
}

func withTestDatabasePortFile(t *testing.T, source string, testfile string, fn func(*testing.T, string)) {
	bytes, err := ioutil.ReadFile(filepath.Join("testdata/databases", testfile))
	if err != nil {
		t.Fatal(err)
	}
	configuration := string(bytes)

	tmpFile, err := ioutil.TempFile(os.TempDir(), "testsrc.*port")
	if err != nil {
		t.Fatal("cannot create temporary file:", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err = tmpFile.WriteString(configuration); err != nil {
		t.Fatal("failed to write to temporary file:", err)
	}

	fn(t, tmpFile.Name())
}

func assertRowCount(t *testing.T, expected int, db *schema.Database, table string) {
	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", db.EscapeIdentifier(table)))
	var count int
	err := row.Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, expected, count, "the number of rows is different than expected")
}

func assertCsvCellContents(t *testing.T, expected string, csvfilename string, row int, col int, msgsAndArgs ...interface{}) {
	csvfile, err := os.Open(csvfilename)
	if err != nil {
		assert.FailNow(t, "%w", err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	rowItr := 0

	for {
		line, err := reader.Read()
		if err == io.EOF {
			assert.FailNow(t, "fewer than %d rows in CSV", row)
		} else if err != nil {
			assert.FailNow(t, "%w", err)
		}

		if row != rowItr {
			rowItr++
			break
		}

		assert.EqualValues(t, expected, line[col], msgsAndArgs...)
		return
	}
}

func setupTable(db *schema.Database, tableName string) {
	switch tableName {
	case "widgets":
		db.Exec(db.GenerateCreateTableStatement("widgets", widgetsTableDefinition))
		importCSV("testsrc", "widgets", "testdata/example_widgets.csv", widgetsTableDefinition.Columns)
	case "objects":
		setupObjectsTable(db)
	case "actions":
		setupActionsTable(db)
	}
}

func setupObjectsTable(db *schema.Database) {
	objects := schema.Table{"", "objects", make([]schema.Column, 3)}
	objects.Columns[0] = schema.Column{"id", schema.INTEGER, map[schema.Option]int{schema.BYTES: 8}}
	objects.Columns[1] = schema.Column{"name", schema.STRING, map[schema.Option]int{schema.LENGTH: 255}}
	objects.Columns[2] = schema.Column{"updated_at", schema.TIMESTAMP, map[schema.Option]int{}}

	db.Exec(db.GenerateCreateTableStatement("objects", &objects))
	statement, _ := db.Prepare("INSERT INTO objects (id, name, updated_at) VALUES (?, ?, ?)")
	statement.Exec(1, "book", time.Now().Add(-7*24*time.Hour))
	statement.Exec(2, "tv", time.Now().Add(-1*24*time.Hour))
	statement.Exec(3, "chair", time.Now())
	statement.Close()
}

func setupActionsTable(db *schema.Database) {
	actions := schema.Table{"", "actions", make([]schema.Column, 3)}
	actions.Columns[0] = schema.Column{"id", schema.INTEGER, map[schema.Option]int{schema.BYTES: 8}}
	actions.Columns[1] = schema.Column{"name", schema.STRING, map[schema.Option]int{schema.LENGTH: 255}}
	actions.Columns[2] = schema.Column{"options", schema.STRING, map[schema.Option]int{schema.LENGTH: 1023}}
	db.Exec(db.GenerateCreateTableStatement("actions", &actions))
}

func actionsInsert(db *schema.Database, args ...interface{}) {
	statement, _ := db.Prepare("INSERT INTO actions (id, name, options) VALUES (?, ?, ?)")
	statement.Exec(args...)
	statement.Close()
}
