package main

import (
	"testing"

	"github.com/rnovales/teleport/schema"
)

func TestSQLTransform(t *testing.T) {
	runDatabaseTest(t, "full.port", func(t *testing.T, _ string, _ *schema.Database, db *schema.Database) {
		setupObjectsTable(db)
		SQLTransforms["test.sql"] = "SELECT name AS title FROM objects"

		updateTransform("testdest", "test")

		assertRowCount(t, 3, db, "test")
		// table, _ := dumpTableMetadata("testdest", "test")
		// assert.Len(t, table.Columns, 1)
		// assert.Equal(t, "title", table.Columns[0].Name)
	})
}
