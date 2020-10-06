package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/rnovales/teleport/schema"
)

func importSqlite3(db *schema.Database, table string, file string, columns []schema.Column) error {
	transaction, err := db.Begin()
	if err != nil {
		return err
	}

	preparedStatement := fmt.Sprintf("INSERT INTO %s (", table)
	for _, column := range columns {
		preparedStatement += fmt.Sprintf("\"%s\", ", column.Name)
	}
	preparedStatement = strings.TrimSuffix(preparedStatement, ", ")

	preparedStatement += ") VALUES ("
	for range columns {
		preparedStatement += "?, "
	}
	preparedStatement = strings.TrimSuffix(preparedStatement, ", ")
	preparedStatement += ");"

	statement, err := transaction.Prepare(preparedStatement)
	if err != nil {
		return err
	}

	csvfile, err := os.Open(file)
	if err != nil {
		return err
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))
	if err != nil {
		return err
	}

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("line error: %w", err)
		}

		writeBuffer := make([]interface{}, len(line))
		for i, value := range line {
			if value == "" { // Assume a blank cell is NULL
				writeBuffer[i] = nil
			} else {
				writeBuffer[i] = value
			}
		}

		_, err = statement.Exec(writeBuffer...)
		if err != nil {
			return fmt.Errorf("import statement exec error: %w", err)
		}
	}

	err = statement.Close()
	if err != nil {
		return err
	}

	err = transaction.Commit()
	if err != nil {
		return err
	}

	return nil
}
