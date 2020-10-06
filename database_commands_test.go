package main

import (
	"testing"

	"github.com/rnovales/teleport/schema"
	"github.com/stretchr/testify/assert"
)

func TestTableExists(t *testing.T) {
	runDatabaseTest(t, "full.port", func(t *testing.T, _ string, db *schema.Database, _ *schema.Database) {
		db.Exec("CREATE TABLE IF NOT EXISTS animals (id integer, name varchar(255))")

		actual, _ := tableExists("testsrc", "does_not_exist")
		assert.False(t, actual)

		actual, _ = tableExists("testsrc", "animals")
		assert.True(t, actual)
	})
}

func TestCreateTable(t *testing.T) {
	runDatabaseTest(t, "full.port", func(t *testing.T, _ string, db *schema.Database, _ *schema.Database) {
		table := widgetsTable()

		assert.NoError(t, createTable("testsrc", "newtable", &table))
		actual, _ := tableExists("testsrc", "newtable")
		assert.True(t, actual)
	})
}

func widgetsTable() schema.Table {
	columns := make([]schema.Column, 0)
	columns = append(columns, schema.Column{"id", schema.INTEGER, map[schema.Option]int{schema.BYTES: 8}})
	columns = append(columns, schema.Column{"name", schema.STRING, map[schema.Option]int{schema.LENGTH: 255}})
	columns = append(columns, schema.Column{"active", schema.BOOLEAN, map[schema.Option]int{}})
	columns = append(columns, schema.Column{"price", schema.DECIMAL, map[schema.Option]int{schema.PRECISION: 10, schema.SCALE: 2}})

	return schema.Table{"source", "widgets", columns}
}
