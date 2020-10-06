package schema

import (
	"strings"
)

var parsePostgresSpecialTypes = func(column sqlColumn) (DataType, map[Option]int, bool) {
	databaseTypeName := strings.ToLower(column.DatabaseTypeName())

	options := make(map[Option]int)
	switch databaseTypeName {
	case "money":
		options[PRECISION] = 16
		options[SCALE] = 2
		return DECIMAL, options, true
	case "inet", "uuid", "cidr", "macaddr":
		options[LENGTH] = 255
		return STRING, options, true
	case "xml", "json":
		return TEXT, options, true
	case "jsonb":
		return VARIANT, options, true
	}

	return "", options, false
}

var postgresDriverExtensions = DriverExtensions{
	ParseSpecialTypes: &parsePostgresSpecialTypes,
}
