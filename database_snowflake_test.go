// +build snowflake

package main

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/rnovales/teleport/schema"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestSnowflakeLoadTest(t *testing.T) {
	fromPostgresToSnowflake(t, func(t *testing.T, srcdb *schema.Database, destdb *schema.Database) {
		srcdb.Exec(srcdb.GenerateCreateTableStatement("widgets", widgetsTableDefinition))
		err := importCSV("testsrc", "widgets", "testdata/example_widgets.csv", widgetsTableDefinition.Columns)
		assert.NoError(t, err)

		// run task twice to test both with no destination table and an already existing destination table
		extractLoadDatabase("testsrc", "testdest", "widgets")
		extractLoadDatabase("testsrc", "testdest", "widgets")

		assert.Equal(t, log.InfoLevel, hook.LastEntry().Level)
		destinationTable, err := destdb.DumpTableMetadata("testsrc_widgets")
		assert.NoError(t, err)
		assertRowCount(t, 10, destdb, "testsrc_widgets")

		var (
			created_at  time.Time
			launched    string
			description string
		)
		row := destdb.QueryRow(`SELECT "created_at", "launched", "description" FROM "testsrc_widgets" WHERE "id" = 1`)
		err = row.Scan(&created_at, &launched, &description)
		assert.NoError(t, err)
		createdAt, err := time.Parse(time.RFC3339, "2020-03-11T23:28:21-06:00")
		assert.NoError(t, err)
		assert.Equal(t, createdAt.UTC(), created_at.UTC())
		assert.Contains(t, launched, "2015-11-06")
		assert.Contains(t, description, "* Officiis. \n* Sapiente.")

		expectedColumns := make([]schema.Column, len(widgetsTableDefinition.Columns))
		copy(expectedColumns, widgetsTableDefinition.Columns)
		expectedColumns[3] = schema.Column{"name", schema.TEXT, map[schema.Option]int{}}
		expectedColumns[7] = schema.Column{"description", schema.TEXT, map[schema.Option]int{}}
		assert.Equal(t, expectedColumns, destinationTable.Columns)
	})
}

func fromPostgresToSnowflake(t *testing.T, testfn func(*testing.T, *schema.Database, *schema.Database)) {
	snowflakeURL := os.ExpandEnv("snowflake://$TEST_SNOWFLAKE_USER:$TEST_SNOWFLAKE_PASSWORD@$TEST_SNOWFLAKE_HOST/$TEST_SNOWFLAKE_DBNAME")
	options := map[string]string{"s3_bucket": "teleportdata", "s3_bucket_region": "us-east-1", "external_stage_name": "PUBLIC.TELEPORTDATA"}

	Databases["testsrc"] = Database{"postgres://postgres@localhost:45432/?sslmode=disable", map[string]string{}, true}
	srcdb, err := connectDatabase("testsrc")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testsrc")

	Databases["testdest"] = Database{snowflakeURL, options, false}
	destdb, err := connectDatabase("testdest")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testdest")

	defer srcdb.Exec(`DROP TABLE IF EXISTS "widgets"`)
	defer destdb.Exec(`DROP TABLE IF EXISTS "testsrc_widgets"`)

	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	defer log.SetLevel(log.GetLevel())
	log.SetLevel(log.DebugLevel)

	log.StandardLogger().ExitFunc = func(int) {}

	hook.Reset()

	testfn(t, srcdb, destdb)

	for _, entry := range hook.Entries {
		t.Log(entry.String())
	}
}
