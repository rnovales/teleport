package schema

import (
	"os"
	"testing"

	_ "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
)

func TestSnowflakeInspection(t *testing.T) {
	withDatabase(t, os.ExpandEnv("snowflake://$TEST_SNOWFLAKE_USER:$TEST_SNOWFLAKE_PASSWORD@$TEST_SNOWFLAKE_HOST/$TEST_SNOWFLAKE_DBNAME"), func(db Database) {
		testColumnCases(t, db, []struct {
			originalDataTypes  []string
			column             Column
			createTabeDataType string
		}{
			// Integer Types
			{
				[]string{"INT", "INTEGER"},
				Column{"", INTEGER, map[Option]int{BYTES: 8}},
				"INTEGER",
			},
			{
				[]string{"BIGINT"},
				Column{"", INTEGER, map[Option]int{BYTES: 8}},
				"INTEGER",
			},
			{
				[]string{"SMALLINT"},
				Column{"", INTEGER, map[Option]int{BYTES: 8}},
				"INTEGER",
			},

			// Decimal Types
			{
				[]string{"DECIMAL(9,4)", "NUMERIC(9,4)"},
				Column{"", DECIMAL, map[Option]int{PRECISION: 9, SCALE: 4}},
				"DECIMAL(9,4)",
			},

			// Float Types
			{
				[]string{"REAL", "FLOAT4", "FLOAT"},
				Column{"", FLOAT, map[Option]int{BYTES: 8}},
				"FLOAT8", // TODO: options support
			},
			{
				[]string{"DOUBLE PRECISION", "FLOAT8"},
				Column{"", FLOAT, map[Option]int{BYTES: 8}},
				"FLOAT8", // TODO: options support
			},

			// Boolean Types
			{
				[]string{"BOOLEAN"},
				Column{"", BOOLEAN, map[Option]int{}},
				"BOOLEAN",
			},

			// Date and Time Types
			{
				[]string{"DATE"},
				Column{"", DATE, map[Option]int{}},
				"DATE",
			},
			{
				[]string{"TIMESTAMP"},
				Column{"", TIMESTAMP, map[Option]int{}},
				"TIMESTAMP",
			},
		})

		testColumnCases(t, db, []struct {
			originalDataTypes  []string
			column             Column
			createTabeDataType string
		}{
			// String Types
			{
				[]string{"VARCHAR(511)", "CHARACTER VARYING(511)"},
				Column{"", TEXT, map[Option]int{}},
				"TEXT",
			},
			{
				[]string{"CHAR(127)"},
				Column{"", TEXT, map[Option]int{}},
				"TEXT",
			},
		})
	})
}

func TestSnowflakeTableGeneration(t *testing.T) {
	withDatabase(t, os.ExpandEnv("snowflake://$TEST_SNOWFLAKE_USER:$TEST_SNOWFLAKE_PASSWORD@$TEST_SNOWFLAKE_HOST/$TEST_SNOWFLAKE_DBNAME"), func(db Database) {
		_, err := db.Exec(db.GenerateCreateTableStatement("new_widgets", &widgetsTable))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		defer db.Exec(`DROP TABLE "new_widgets"`)

		table, err := db.DumpTableMetadata("new_widgets")
		if err != nil {
			assert.FailNow(t, err.Error())
		}

		for idx, widgetsColumn := range widgetsTable.Columns {
			dumpedColumn := table.Columns[idx]

			switch widgetsColumn.DataType {
			case STRING:
				assert.Equal(t, widgetsColumn.Name, dumpedColumn.Name)
				assert.Equal(t, TEXT, dumpedColumn.DataType)
				assert.Equal(t, map[Option]int{}, dumpedColumn.Options)
			default:
				assert.Equal(t, widgetsColumn.Name, dumpedColumn.Name)
				assert.Equal(t, widgetsColumn.DataType, dumpedColumn.DataType)
				assert.Equal(t, widgetsColumn.Options, dumpedColumn.Options)
			}
		}
	})
}
