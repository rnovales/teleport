package schema

import (
	"strings"
)

var parseSnowflakeSpecialTypes = func(column sqlColumn) (DataType, map[Option]int, bool) {
	databaseTypeName := strings.ToLower(column.DatabaseTypeName())

	options := make(map[Option]int)
	switch databaseTypeName {
	case "fixed":
		determinedOptions, err := determineOptions(column, DECIMAL)
		if err != nil {
			return "", options, false
		} else if determinedOptions[SCALE] == 0 {
			options[BYTES] = 8
			return INTEGER, options, true
		} else {
			return DECIMAL, determinedOptions, true
		}
	case "variant":
		return VARIANT, options, true
	}

	return "", options, false
}

var generateSnowflakeDataTypeExpression = func(column Column) (string, bool) {
	switch column.DataType {

	// From https://docs.snowflake.com/en/sql-reference/data-types-numeric.html
	// Precision (total number of digits) does not impact storage. In other words, the storage requirements for the same number in columns with different precisions, such as NUMBER(2,0) and NUMBER(38,0), are the same.
	case INTEGER:
		return "INTEGER", true

	// From https://docs.snowflake.com/en/sql-reference/data-types-text.html#string-binary-data-types:
	// There is no performance difference between using the full-length VARCHAR declaration VARCHAR(16777216) or a smaller size
	case TEXT, STRING:
		return "TEXT", true
	}

	return "", false
}

var snowflakeDriverExtensions = DriverExtensions{
	ParseSpecialTypes:          &parseSnowflakeSpecialTypes,
	GenerateDataTypeExpression: &generateSnowflakeDataTypeExpression,
}
