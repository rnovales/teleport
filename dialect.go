package main

import (
	"fmt"
	"strings"

	"github.com/rnovales/teleport/schema"
)

type Dialect struct {
	Key                     string
	HumanName               string
	TerminalCommand         string
	CreateStagingTableQuery string
	FullLoadQuery           string
	ModifiedOnlyLoadQuery   string
	SetSchemaQuery          string
}

var (
	mysql    = Dialect{"mysql", "MySQL", "", "", "", "", ""}
	postgres = Dialect{"postgres", "PostgreSQL", "psql",
		"CREATE TABLE %[2]s AS TABLE %[1]s WITH NO DATA",
		`
			DELETE FROM %[1]s;
			INSERT INTO %[1]s SELECT * FROM %[2]s
		`,
		`
			DELETE FROM %[1]s WHERE %[3]s IN (SELECT %[3]s FROM %[2]s);
			INSERT INTO %[1]s SELECT * FROM %[2]s;
			DROP TABLE %[2]s
		`,
		"SET search_path TO %s"}
	redshift = Dialect{"redshift", "AWS RedShift", "psql",
		"CREATE TEMPORARY TABLE %[2]s (LIKE %[1]s)",
		`
			DELETE FROM %[1]s;
			INSERT INTO %[1]s SELECT * FROM %[2]s
		`,
		`
			DELETE FROM %[1]s USING %[2]s WHERE %[1]s.%[3]s = %[2]s.%[3]s;
			INSERT INTO %[1]s SELECT * FROM %[2]s
		`,
		postgres.SetSchemaQuery}
	snowflake = Dialect{"snowflake", "Snowflake", "psql",
		"CREATE TEMPORARY TABLE %[2]s LIKE %[1]s",
		`
			TRUNCATE TABLE %[1]s;
			INSERT INTO %[1]s SELECT * FROM %[2]s
		`,
		`
			MERGE INTO %[1]s USING %[2]s ON %[1]s.%[3]s = %[2]s.%[3]s;
		`,
		"USE SCHEMA %s"}
	sqlite = Dialect{"sqlite", "SQLite3", "",
		"CREATE TABLE %[2]s AS SELECT * FROM %[1]s LIMIT 0", postgres.FullLoadQuery, postgres.ModifiedOnlyLoadQuery, ""}
)

func GetDialect(db *schema.Database) Dialect {
	switch db.Driver {
	case "postgres":
		return postgres
	case "sqlite3":
		return sqlite
	case "mysql":
		return mysql
	case "redshift":
		return redshift
	case "snowflake":
		return snowflake
	default:
		return mysql
	}
}

func getSnowFlakeModifiedOnlyLoadQuery(db *schema.Database, stagingTableName string, destinationTable *schema.Table, strategyOpts StrategyOptions) string {
	var q, targetCol, targetVal strings.Builder
	q.WriteString(fmt.Sprintf("MERGE INTO %[1]s USING %[2]s ON %[1]s.%[3]s = %[2]s.%[3]s\nWHEN MATCHED THEN UPDATE SET\n", db.EscapeIdentifier(destinationTable.Name), db.EscapeIdentifier(stagingTableName), db.EscapeIdentifier(strategyOpts.PrimaryKey)))

	for i, c := range destinationTable.Columns {
		q.WriteString(fmt.Sprintf("%[1]s.%[3]s = %[2]s.%[3]s", db.EscapeIdentifier(destinationTable.Name), db.EscapeIdentifier(stagingTableName), db.EscapeIdentifier(c.Name)))

		targetCol.WriteString(db.EscapeIdentifier(c.Name))
		targetVal.WriteString(fmt.Sprintf("%s.%s", db.EscapeIdentifier(stagingTableName), db.EscapeIdentifier(c.Name)))
		if i != len(destinationTable.Columns) - 1 {
			q.WriteString(", \n")
			targetCol.WriteString(", \n")
			targetVal.WriteString(", \n")
		}


	}

	q.WriteString("\nWHEN NOT MATCHED THEN INSERT (\n" + targetCol.String() + ") \n VALUES (\n" + targetVal.String() + ")")

	return q.String()
}
