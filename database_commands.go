package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"

	"github.com/rnovales/teleport/schema"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

func tableExists(source string, tableName string) (bool, error) {
	db, err := connectDatabase(source)
	if err != nil {
		return false, err
	}

	tables, err := db.TableNames()
	if err != nil {
		return false, err
	}

	for _, table := range tables {
		if table == tableName {
			return true, nil
		}
	}

	return false, nil
}

func createTable(source string, tableName string, table *schema.Table) error {
	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Connect Error:", err)
	}

	statement := db.GenerateCreateTableStatement(tableName, table)

	_, err = db.Exec(statement)

	return err
}

func listTables(source string) {
	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	tables, err := db.TableNames()
	if err != nil {
		log.Fatal("Database Error:", err)
	}
	for _, tablename := range tables {
		fmt.Println(tablename)
	}
}

func dropTable(source string, table string) {
	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	exists, err := tableExists(source, table)
	if err != nil {
		log.Fatal(err)
	} else if !exists {
		log.Fatalf("table \"%s\" not found in \"%s\"", table, source)
	}

	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", db.EscapeIdentifier(table)))
	if err != nil {
		log.Fatal(err)
	}
}

func createDestinationTable(source string, destination string, sourceTableName string) {
	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	table, err := db.DumpTableMetadata(sourceTableName)
	if err != nil {
		log.Fatal("Table Metadata Error:", err)
	}

	err = createTable(destination, fmt.Sprintf("%s_%s", source, sourceTableName), table)

	if err != nil {
		log.Fatal(err)
	}
}

func createDestinationTableFromConfigFile(source string, file string) error {
	table := readTableFromConfigFile(file)

	return createTable(source, fmt.Sprintf("%s_%s", table.Source, table.Name), table)
}

func aboutDB(source string) {
	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Info("database successfully connected ✓")
	}

	fmt.Println("Name: ", source)
	fmt.Printf("Type: %s\n", GetDialect(db).HumanName)
}

func databaseTerminal(source string) {
	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal(err)
	}

	command := GetDialect(db).TerminalCommand
	if command == "" {
		log.Fatalf("Not implemented for this database type")
	}

	binary, err := exec.LookPath(command)
	if err != nil {
		log.Fatalf("command exec err (%s): %s", command, err)
	}

	env := os.Environ()

	err = syscall.Exec(binary, []string{command, Databases[source].URL}, env)
	if err != nil {
		log.Fatalf("Syscall error: %s", err)
	}

}

func describeTable(source string, tableName string) {
	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	table, err := db.DumpTableMetadata(tableName)
	if err != nil {
		log.Fatal("Describe Table Error:", err)
	}

	fmt.Println("Source: ", table.Source)
	fmt.Println("Table: ", table.Name)
	fmt.Println()
	fmt.Println("schema.Columns:")
	fmt.Println("========")
	for _, column := range table.Columns {
		fmt.Print(column.Name, " | ", column.DataType)
		if len(column.Options) > 0 {
			fmt.Print(" ( ")
			for option, value := range column.Options {
				fmt.Print(option, ": ", value, ", ")

			}
			fmt.Print(" )")
		}
		fmt.Println()
	}
}

func tableMetadata(source string, tableName string) {
	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	table, err := db.DumpTableMetadata(tableName)
	if err != nil {
		log.Fatal("Describe Table Error:", err)
	}

	b, err := yaml.Marshal(table)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(b))
}

func readTableFromConfigFile(file string) *schema.Table {
	var table schema.Table

	yamlFile, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("yamlFile.Get err   #%v ", err)
	}

	err = yaml.Unmarshal(yamlFile, &table)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return &table
}
