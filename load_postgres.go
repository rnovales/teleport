package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"time"

	"github.com/rnovales/teleport/schema"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

func importPostgres(db *schema.Database, table string, file string, columns []schema.Column) error {
	transaction, err := db.Begin()
	if err != nil {
		return err
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	statement, err := transaction.Prepare(pq.CopyIn(table, columnNames...))
	if err != nil {
		return err
	}

	csvfile, err := os.Open(file)
	if err != nil {
		return err
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		writeBuffer := make([]interface{}, len(line))
		for i, value := range line {
			switch {
			case value == "": // Assume a blank cell is NULL
				writeBuffer[i] = nil
			case columns[i].DataType == schema.TIMESTAMP:
				if t, err := time.Parse(time.RFC3339, value); err == nil {
					writeBuffer[i] = t.UTC().Format(time.RFC3339)
				} else {
					log.Warnf("unable to parse timestamp for column '%s': '%s'. Please use RFC3339 timestamp formats (e.g., '')", columns[i].Name, value)
					writeBuffer[i] = value
				}
			default:
				writeBuffer[i] = value
			}
		}

		_, err = statement.Exec(writeBuffer...)
		if err != nil {
			return err
		}
	}

	_, err = statement.Exec()
	if err != nil {
		return err
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
