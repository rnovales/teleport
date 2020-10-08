package main

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"

	slutil "github.com/hundredwatt/starlib/util"
	"github.com/rnovales/teleport/schema"
	log "github.com/sirupsen/logrus"
	"github.com/xo/dburl"
	"go.starlark.net/starlark"
)

var (
	dbs = make(map[string]*schema.Database)
)

func extractLoadDatabase(sourceOrPath string, destination string, tableName string) {
	var source string
	if strings.Contains(sourceOrPath, "/") {
		source = fileNameWithoutExtension(filepath.Base(sourceOrPath))
	} else {
		source = sourceOrPath
	}

	fnlog := log.WithFields(log.Fields{
		"from":  source,
		"to":    destination,
		"table": tableName,
	})
	fnlog.Info("Starting extract-load")

	var sourceTable schema.Table
	var destinationTable schema.Table
	var columns []schema.Column
	var tableExtract TableExtract
	var csvfile string

	destinationTableName := strings.ToUpper(tableName)
	fnlog.Info(fmt.Sprintf("destination table name = %s", destinationTableName))

	RunWorkflow([]func() error{
		func() error { return readTableExtractConfiguration(sourceOrPath, tableName, &tableExtract) },
		func() error { return connectDatabaseWithLogging(source) },
		func() error { return connectDatabaseWithLogging(destination) },
		func() error { return inspectTable(source, tableName, &sourceTable, &tableExtract) },
		func() error {
			return createDestinationTableIfNotExists(destination, destinationTableName, &sourceTable, &destinationTable)
		},
		func() error {
			return extractSource(&sourceTable, &destinationTable, tableExtract, &columns, &csvfile, false)
		},
		func() error { return load(&destinationTable, &columns, &csvfile, tableExtract.strategyOpts()) },
	}, func() {
		fnlog.WithField("rows", currentWorkflow.RowCounter).Info("Completed extract-load 🎉")
	})
}

func extractDatabase(sourceOrPath string, tableName string) {
	var source string
	if strings.Contains(sourceOrPath, "/") {
		source = fileNameWithoutExtension(filepath.Base(sourceOrPath))
	} else {
		source = sourceOrPath
	}

	log.WithFields(log.Fields{
		"from":  source,
		"table": tableName,
	}).Info("Extracting table data to CSV")

	var table schema.Table
	var tableExtract TableExtract
	var csvfile string

	RunWorkflow([]func() error{
		func() error { return readTableExtractConfiguration(sourceOrPath, tableName, &tableExtract) },
		func() error { return connectDatabaseWithLogging(source) },
		func() error { return inspectTable(source, tableName, &table, &tableExtract) },
		func() error { return extractSource(&table, nil, tableExtract, nil, &csvfile, true) },
	}, func() {
		log.WithFields(log.Fields{
			"file": csvfile,
			"rows": currentWorkflow.RowCounter,
		}).Info("Extract to CSV completed 🎉")
	})
}

func connectDatabaseWithLogging(source string) (err error) {
	log.WithFields(log.Fields{
		"database": source,
	}).Debug("Establish connection to Database")

	_, err = connectDatabase(source)

	return
}

func inspectTable(source string, tableName string, table *schema.Table, tableExtract *TableExtract) (err error) {
	log.WithFields(log.Fields{
		"database": source,
		"table":    tableName,
	}).Debug("Inspecting Table")

	db, _ := connectDatabase(source)

	dumpedTable, err := db.DumpTableMetadata(tableName)
	if err != nil {
		return err
	}
	dumpedTable.Source = source

	if tableExtract != nil {
		for i, column := range dumpedTable.Columns {
			if columnTransform, ok := tableExtract.ColumnTransforms[column.Name]; ok {
				if columnTransform.Type != "" {
					dataType, options, schemaErr := db.ParseDatabaseTypeFromString(columnTransform.Type)
					if schemaErr != nil {
						return
					}
					dumpedTable.Columns[i].DataType = dataType
					dumpedTable.Columns[i].Options = options
				}
			}
		}

		for _, computedColumn := range tableExtract.ComputedColumns {
			column, err := computedColumn.toColumn()
			if err != nil {
				return err
			}
			dumpedTable.Columns = append(dumpedTable.Columns, column)
		}
	}

	*table = *dumpedTable
	return
}

func extractSource(sourceTable *schema.Table, destinationTable *schema.Table, tableExtract TableExtract, columns *[]schema.Column, csvfile *string, includeHeaders bool) (err error) {
	log.WithFields(log.Fields{
		"database": sourceTable.Source,
		"table":    sourceTable.Name,
		"type":     tableExtract.LoadOptions.Strategy,
	}).Debug("Exporting CSV of table data")

	var importColumns []schema.Column
	var exportColumns []schema.Column
	var computedColumns []ComputedColumn

	if destinationTable != nil {
		importColumns = importableColumns(destinationTable, sourceTable)
	} else {
		importColumns = sourceTable.Columns
	}
	for _, column := range importColumns {
		if column.Options[schema.COMPUTED] != 1 {
			exportColumns = append(exportColumns, column)
		} else {
			for _, computedColumn := range tableExtract.ComputedColumns {
				if computedColumn.Name == column.Name {
					computedColumns = append(computedColumns, computedColumn)
					continue
				}
			}
		}
	}

	var whereStatement string
	switch tableExtract.LoadOptions.Strategy {
	case Full:
		whereStatement = ""
	case ModifiedOnly:
		updateTime := (time.Now().Add(time.Duration(-1*tableExtract.LoadOptions.GoBackHours) * time.Hour)).Format("2006-01-02 15:04:05")
		whereStatement = fmt.Sprintf("%s > '%s'", tableExtract.LoadOptions.ModifiedAtColumn, updateTime)
	}

	file, err := exportCSV(sourceTable.Source, sourceTable.Name, exportColumns, whereStatement, tableExtract, includeHeaders)
	if err != nil {
		return err
	}

	*csvfile = file
	if columns != nil {
		*columns = importColumns
	}
	return
}

func exportCSV(source string, table string, columns []schema.Column, whereStatement string, tableExtract TableExtract, includeHeaders bool) (string, error) {
	db, err := connectDatabase(source)
	if err != nil {
		return "", fmt.Errorf("Database Open Error: %w", err)
	}

	exists, err := tableExists(source, table)
	if err != nil {
		return "", err
	} else if !exists {
		return "", fmt.Errorf("table \"%s\" not found in \"%s\"", table, source)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	escapedColumnNames := make([]string, len(columns))
	for i, columnName := range columnNames {
		escapedColumnNames[i] = db.EscapeIdentifier(columnName)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(escapedColumnNames, ", "), db.EscapeIdentifier(table))
	if whereStatement != "" {
		query += fmt.Sprintf(" WHERE %s", whereStatement)
	}

	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}

	headers := make([]string, len(columnNames)+len(tableExtract.ComputedColumns))
	copy(headers, columnNames)
	for i, computedColumn := range tableExtract.ComputedColumns {
		headers[len(columnNames)+i] = computedColumn.Name
	}

	return generateCSV(headers, fmt.Sprintf("extract-%s-%s-*.csv", table, source), includeHeaders, func(writer *csv.Writer) error {
		destination := make([]interface{}, len(columnNames))
		rawResult := make([]interface{}, len(columnNames))
		writeBuffer := make([]string, len(columnNames)+len(tableExtract.ComputedColumns))
		for i := range rawResult {
			destination[i] = &rawResult[i]
		}

		done := monitorExtractDB()
		defer done()

		for rows.Next() {
			err := rows.Scan(destination...)
			if err != nil {
				return err
			}

			IncrementRowCounter()

			for i := range columns {
				value, err := applyColumnTransforms(parseFromDatabase(rawResult[i], columns[i].DataType), tableExtract.ColumnTransforms[columns[i].Name].Functions)
				if err != nil {
					return err
				}

				writeBuffer[i] = formatForDatabaseCSV(value, columns[i].DataType)
			}

			if len(tableExtract.ComputedColumns) > 0 {
				row := make(map[string]interface{})
				for i := range columns {
					row[columns[i].Name] = parseFromDatabase(rawResult[i], columns[i].DataType)
				}

				arg, err := slutil.Marshal(row)
				if err != nil {
					return err
				}

				for j, computedColumn := range tableExtract.ComputedColumns {
					i := len(columns) + j

					value, err := computeColumn(arg, columns, computedColumn)
					if err != nil {
						return err
					}

					computedColumnColumn, err := computedColumn.toColumn()
					if err != nil {
						return err
					}

					writeBuffer[i] = formatForDatabaseCSV(value, computedColumnColumn.DataType)
				}
			}

			err = writer.Write(writeBuffer)
			if err != nil {
				return err
			}

			if Preview && GetRowCounter() >= int64(PreviewLimit) {
				break
			}
		}

		return nil
	})
}

func connectDatabase(source string) (*schema.Database, error) {
	if dbs[source] != nil {
		return dbs[source], nil
	}

	url := Databases[source].URL

	u, err := dburl.Parse(url)
	if err != nil {
		return nil, err
	}

	if u.Driver == "mysql" {
		err := registerRDSMysqlCerts()
		if err != nil {
			return nil, err
		}

		mysqlconfig, err := mysqldriver.ParseDSN(u.DSN)
		if err != nil {
			return nil, err
		}

		mysqlconfig.ParseTime = true

		u.DSN = mysqlconfig.FormatDSN()
	}

	database, err := sql.Open(u.Driver, u.DSN)
	if err != nil {
		return nil, err
	}

	err = database.Ping()
	if err != nil {
		return nil, err
	}

	var driver string
	if strings.HasPrefix(Databases[source].URL, "redshift://") {
		driver = "redshift"
	} else if strings.HasPrefix(Databases[source].URL, "rs://") {
		driver = "redshift"
	} else {
		driver = u.Driver
	}

	db := &schema.Database{database, driver}

	schemaQuery := GetDialect(db).SetSchemaQuery
	if schema, ok := databaseSchema(source, driver); ok {
		db.Exec(fmt.Sprintf(schemaQuery, pq.QuoteIdentifier(schema)))
	}

	if schema, ok := Databases[source].Options["schema"]; ok {
		_, err := database.Exec(fmt.Sprintf(schemaQuery, pq.QuoteIdentifier(schema)))
		if err != nil {
			return nil, err
		}
	}

	dbs[source] = db
	return dbs[source], nil
}

func applyColumnTransforms(value interface{}, columnTransforms []*starlark.Function) (interface{}, error) {
	if len(columnTransforms) == 0 {
		return value, nil
	}

	slvalue, err := slutil.Marshal(value)
	if err != nil {
		return "", err
	}

	for _, function := range columnTransforms {
		slvalue, err = starlark.Call(GetThread(), function, starlark.Tuple{slvalue}, nil)
		if err != nil {
			return "", appendBackTraceToStarlarkError(err)
		}
	}

	value, err = slutil.Unmarshal(slvalue)
	if err != nil {
		return "", err
	}

	return value, nil
}

func computeColumn(arg starlark.Value, columns []schema.Column, computedColumn ComputedColumn) (interface{}, error) {
	slvalue, err := starlark.Call(GetThread(), computedColumn.Function, starlark.Tuple{arg}, nil)
	if err != nil {
		return "", appendBackTraceToStarlarkError(err)
	}

	value, err := slutil.Unmarshal(slvalue)
	if err != nil {
		return "", err
	}

	return value, nil
}

func monitorExtractDB() func() {
	// only enable monitoring when log level is set to DEBUG
	if log.GetLevel() != log.DebugLevel {
		return func() {}
	}

	ticker := time.NewTicker(15 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				log.WithFields(log.Fields{
					"rows":         currentWorkflow.RowCounter,
					"csvBytesSize": byteCountDecimal(currentWorkflow.BytesCounter),
				}).Debug("Exporting CSV")
			}
		}
	}()

	return func() {
		ticker.Stop()
		done <- true
	}
}

func formatForDatabaseCSV(value interface{}, dataType schema.DataType) string {
	if dataType == schema.DATE {
		switch value.(type) {
		case string:
			return value.(string)
		case time.Time:
			return value.(time.Time).Format("2006-01-02")
		}
	}

	return formatForCSV(value)
}

func parseFromDatabase(value interface{}, dataType schema.DataType) interface{} {
	switch value.(type) {
	case []uint8:
		str := string(value.([]uint8))
		switch dataType {
		case schema.INTEGER:
			if int, err := strconv.ParseInt(str, 10, 64); err == nil {
				return int
			} else {
				return str
			}
		default:
			return str
		}
	default:
		return value
	}
}

func registerRDSMysqlCerts() error {
	pem, err := Asset("rds-combined-ca-bundle.pem")
	if err != nil {
		return err
	}

	rootCertPool := x509.NewCertPool()
	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
		return errors.New("couldn't append certs from pem")
	}

	err = mysqldriver.RegisterTLSConfig("rds", &tls.Config{RootCAs: rootCertPool, InsecureSkipVerify: true})
	if err != nil {
		return err
	}
	return nil
}
