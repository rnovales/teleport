package main

import (
	"testing"
	"time"

	"github.com/rnovales/teleport/schema"
	"github.com/stretchr/testify/assert"
	"go.starlark.net/starlark"
)

func TestMySQLExportTimestampToDate(t *testing.T) {
	currentWorkflow = &Workflow{Thread: &starlark.Thread{}}

	withMySQLDatabase(t, func(t *testing.T, srcdb *schema.Database) {
		columns := make([]schema.Column, 1)
		columns[0] = schema.Column{"updated_at", schema.DATE, map[schema.Option]int{}}
		csvfile, err := exportCSV("testsrc", "objects", columns, "", TableExtract{}, false)
		assert.NoError(t, err)

		assertCsvCellContents(t, time.Now().UTC().Add(-7*24*time.Hour).Format("2006-01-02"), csvfile, 1, 0)
	})
}

func TestMySQLWithAmazonRDSSSL(t *testing.T) {
	Databases["testrds"] = Database{"mysql://test.rds.amazonaws.com:43306/test_db?tls=rds", map[string]string{}, true}

	// Since this is a fake host name, we expect a tcp error; if tls=rds is not working, a different error would occur
	_, err := connectDatabase("testrds")
	assert.Error(t, err)
	assert.Equal(t, "dial tcp: lookup test.rds.amazonaws.com: no such host", err.Error())
}

func TestMySQLColumnTransforms(t *testing.T) {
	cases := []struct {
		testFile                       string
		table                          string
		transformedColumnName          string
		transformedColumnIndex         int
		transformedColumnFirstRowValue string
	}{
		{
			"transform_column_mysql.port",
			"objects",
			"updated_on",
			3,
			time.Now().Add(-7 * 24 * time.Hour).UTC().Format("2006-01-02"),
		},
	}

	for _, cse := range cases {
		t.Run(cse.testFile, func(t *testing.T) {
			withMySQLDatabase(t, func(t *testing.T, db *schema.Database) {
				withTestDatabasePortFile(t, "testsrc", cse.testFile, func(t *testing.T, portFile string) {
					setupTable(db, cse.table)

					extractDatabase(portFile, cse.table)

					csvfile := hook.LastEntry().Data["file"].(string)
					assertCsvCellContents(t, cse.transformedColumnFirstRowValue, csvfile, 1, cse.transformedColumnIndex, "`%s` column value not equal", cse.transformedColumnName)
				})
			})
		})
	}
}

func withMySQLDatabase(t *testing.T, testfn func(*testing.T, *schema.Database)) {
	Databases["testsrc"] = Database{"mysql://mysql_test_user:password@localhost:43306/test_db", map[string]string{}, true}
	srcdb, err := connectDatabase("testsrc")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testsrc")

	setupObjectsTable(srcdb)
	defer srcdb.Exec(`DROP TABLE IF EXISTS objects`)

	redirectLogs(t, func() {
		testfn(t, srcdb)
	})
}
