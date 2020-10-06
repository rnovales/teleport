package schema

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	xschema "github.com/jimsmart/schema"
)

type Database struct {
	*sql.DB
	Driver string
}

type DriverExtensions struct {
	ParseSpecialTypes          *func(sqlColumn) (DataType, map[Option]int, bool)
	GenerateDataTypeExpression *func(Column) (string, bool)
	EscapeIdentifier           *func(string) string
}

var GenericDatabase = &Database{}

// Table is our representation of a Table in a relational database
type Table struct {
	Source  string   `yaml:"source"`
	Name    string   `yaml:"table"`
	Columns []Column `yaml:"columns"`
}

// Column is our representation of a column within a table
type Column struct {
	Name     string         `yaml:"name"`
	DataType DataType       `yaml:"datatype"`
	Options  map[Option]int `yaml:"options"`
}

type DataType string

const (
	// Numeric types
	INTEGER DataType = "integer"
	DECIMAL DataType = "decimal"
	FLOAT   DataType = "float"
	NUMBER  DataType = "number"

	// String types
	STRING DataType = "string"
	TEXT   DataType = "text"

	// Time types
	DATE         DataType = "date"
	TIMESTAMP    DataType = "timestamp"
	TIMESTAMPTZ  DataType = "timestamptz"

	// Other types
	BOOLEAN DataType = "boolean"

	VARIANT DataType = "variant"
)

type Option string

const (
	LENGTH    Option = "length"
	PRECISION Option = "precision"
	SCALE     Option = "scale"
	BYTES     Option = "bytes"
	COMPUTED  Option = "computed"
)

// MaxLength represents the maximum possible length for the data type
const MaxLength int = -1

var (
	// ErrColumnNotSupported indicates Teleport does not currently support this column and will ignore it
	ErrColumnNotSupported = fmt.Errorf("column not supported")
)

func (db *Database) TableNames() ([]string, error) {
	return xschema.TableNames(db.DB)
}

func (db *Database) DumpTableMetadata(tableName string) (*Table, error) {
	table := Table{"", tableName, nil}
	columnTypes, err := xschema.Table(db.DB, tableName)
	if err != nil {
		return nil, err
	}

	for _, columnType := range columnTypes {
		column := Column{}
		column.Name = columnType.Name()
		column.DataType, column.Options, err = db.ParseDatabaseType(columnType)
		if err != nil {
			switch err {
			case ErrColumnNotSupported:
				continue
			default:
				return nil, err
			}
		}
		table.Columns = append(table.Columns, column)
	}

	return &table, nil
}

func (db *Database) ParseDatabaseType(column sqlColumn) (DataType, map[Option]int, error) {
	options := make(map[Option]int)

	if db.driverExtensions().ParseSpecialTypes != nil {
		dataType, options, ok := (*db.driverExtensions().ParseSpecialTypes)(column)
		if ok {
			return dataType, options, nil
		}
	}

	dataType, err := determineDataType(column)
	if err != nil {
		return dataType, options, err
	}

	options, err = determineOptions(column, dataType)
	if err != nil {
		return dataType, options, err
	}

	return dataType, options, nil
}

func (db *Database) ParseDatabaseTypeFromString(databaseType string) (DataType, map[Option]int, error) {
	computedColumn := computedColumn{"placeholder", databaseType}

	return db.ParseDatabaseType(&computedColumn)
}

func (db *Database) EscapeIdentifier(name string) string {
	if db.driverExtensions().EscapeIdentifier != nil {
		return (*db.driverExtensions().EscapeIdentifier)(name)
	}
	return fmt.Sprintf("\"%s\"", name)
}

func (db *Database) driverExtensions() DriverExtensions {
	switch db.Driver {
	case "mysql":
		return mysqlDriverExtensions
	case "redshift":
		return redshiftDriverExtensions
	case "postgres":
		return postgresDriverExtensions
	case "snowflake":
		return snowflakeDriverExtensions
	default:
		return DriverExtensions{}
	}
}

func determineDataType(columnType sqlColumn) (DataType, error) {
	databaseTypeName := strings.ToLower(columnType.DatabaseTypeName())
	intRegex := regexp.MustCompile(`^(big|small|medium)?(int|integer|serial)(2|4|8)?$`)
	floatRegex := regexp.MustCompile(`^(float|double( precision)?|real)(4|8)?$`)
	decimalRegex := regexp.MustCompile(`^(numeric|decimal)`)
	stringRegex := regexp.MustCompile(`(var)?char(acter)?( varying)?`)

	switch {
	case intRegex.MatchString(databaseTypeName):
		return NUMBER, nil
	case floatRegex.MatchString(databaseTypeName):
		return FLOAT, nil
	case decimalRegex.MatchString(databaseTypeName):
		return DECIMAL, nil
	case stringRegex.MatchString(databaseTypeName):
		return STRING, nil
	case strings.Contains(databaseTypeName, "text"):
		return TEXT, nil
	case databaseTypeName == "num": // "NUM" data type in SQLite
		return TEXT, nil
	case strings.Contains(databaseTypeName, "tinyint"):
		return BOOLEAN, nil
	case strings.HasPrefix(databaseTypeName, "bool"):
		return BOOLEAN, nil
	case strings.HasPrefix(databaseTypeName, "datetime"):
		return TIMESTAMP, nil
	case databaseTypeName == "timestamptz" || databaseTypeName == "timestamp with time zone":
		return TIMESTAMPTZ, nil
	case strings.HasPrefix(databaseTypeName, "timestamp"):
		return TIMESTAMP, nil
	case databaseTypeName == "date":
		return DATE, nil
	}

	log.Warnf("unable to determine data type for: %s %s", columnType.Name(), databaseTypeName)
	return "", ErrColumnNotSupported
}

func determineOptions(columnType sqlColumn, dataType DataType) (map[Option]int, error) {
	options := make(map[Option]int)
	optionsRegex, _ := regexp.Compile("\\((\\d+)(,(\\d+))?\\)$")
	switch dataType {
	case INTEGER:
		options[BYTES] = 8 // Default everything to largest, TODO: revisit
	case FLOAT:
		options[BYTES] = 8 // Default everything to largest, TODO: revisit
	case STRING:
		length, ok := columnType.Length()
		if ok {
			options[LENGTH] = int(length)
		} else if optionsRegex.MatchString(columnType.DatabaseTypeName()) {
			length, err := strconv.Atoi(optionsRegex.FindStringSubmatch(columnType.DatabaseTypeName())[1])
			if err != nil {
				return nil, err
			}
			options[LENGTH] = length
		} else {
			options[LENGTH] = MaxLength
		}
	case DECIMAL:
		precision, scale, ok := columnType.DecimalSize()
		if ok {
			options[PRECISION] = int(precision)
			options[SCALE] = int(scale)
		} else if optionsRegex.MatchString(columnType.DatabaseTypeName()) {
			precision, err := strconv.Atoi(optionsRegex.FindStringSubmatch(columnType.DatabaseTypeName())[1])
			if err != nil {
				return nil, err
			}

			scale, err := strconv.Atoi(optionsRegex.FindStringSubmatch(columnType.DatabaseTypeName())[3])
			if err != nil {
				return nil, err
			}
			options[PRECISION] = precision
			options[SCALE] = scale
		} else {
			log.Warnf("unable to determine options for: %s %s", columnType.Name(), columnType.DatabaseTypeName())
			return nil, ErrColumnNotSupported
		}
	}

	return options, nil
}

func (db *Database) GenerateCreateTableStatement(name string, table *Table) string {
	statement := fmt.Sprintf("CREATE TABLE %s (\n", db.EscapeIdentifier(name))
	for _, column := range table.Columns {
		statement += fmt.Sprintf("%s %s,\n", db.EscapeIdentifier(column.Name), db.GenerateDataTypeExpression(column))
	}
	statement = strings.TrimSuffix(statement, ",\n")
	statement += "\n)"

	return statement
}

func (db *Database) GenerateDataTypeExpression(column Column) string {
	if db.driverExtensions().GenerateDataTypeExpression != nil {
		expression, ok := (*db.driverExtensions().GenerateDataTypeExpression)(column)
		if ok {
			return expression
		}
	}
	return column.generateDataTypeExpression()
}

func (column *Column) generateDataTypeExpression() string {
	switch column.DataType {
	case INTEGER:
		bytes := column.Options[BYTES]

		return fmt.Sprintf("INT%d", bytes)
	case FLOAT:
		bytes := column.Options[BYTES]

		return fmt.Sprintf("FLOAT%d", bytes)
	case STRING:
		length := column.Options[LENGTH]

		// For columns with LENGTH = max, use 8192 characters for now
		if length == MaxLength {
			length = 16380
		}

		return fmt.Sprintf("VARCHAR(%d)", length)
	case DECIMAL:
		precision := column.Options[PRECISION]
		scale := column.Options[SCALE]

		return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
	}

	return strings.ToUpper(string(column.DataType))
}

func (table *Table) ContainsColumnWithSameName(c Column) bool {
	for _, column := range table.Columns {
		if c.Name == column.Name {
			return true
		}
	}
	return false
}

func (table *Table) NotContainsColumnWithSameName(c Column) bool {
	for _, column := range table.Columns {
		if c.Name == column.Name {
			return false
		}
	}
	return true
}

type sqlColumn interface {
	DatabaseTypeName() string
	Name() string
	Length() (int64, bool)
	DecimalSize() (int64, int64, bool)
}

type computedColumn struct {
	name             string
	databaseTypeName string
}

func (cc *computedColumn) Name() string {
	return cc.name
}

func (cc *computedColumn) DatabaseTypeName() string {
	return cc.databaseTypeName
}

func (cc *computedColumn) Length() (int64, bool) {
	return -1, false
}

func (cc *computedColumn) DecimalSize() (int64, int64, bool) {
	return -1, -1, false
}
