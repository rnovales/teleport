package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateCreateTableStatement(t *testing.T) {
	table := widgetsTable()
	expected := squish(`CREATE TABLE source_widgets (
		id INT8,
		name VARCHAR(255),
		active BOOLEAN,
		price DECIMAL(10,2)
	);`)
	assert.Equal(t, expected, squish(table.GenerateCreateTableStatement("source_widgets")))

}

func widgetsTable() Table {
	columns := make([]Column, 0)
	columns = append(columns, Column{"id", INTEGER, map[Option]int{BYTES: 8}})
	columns = append(columns, Column{"name", STRING, map[Option]int{LENGTH: 255}})
	columns = append(columns, Column{"active", BOOLEAN, map[Option]int{}})
	columns = append(columns, Column{"price", DECIMAL, map[Option]int{PRECISION: 10, SCALE: 2}})

	return Table{"source", "widgets", columns}
}