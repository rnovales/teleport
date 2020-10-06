package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rnovales/teleport/schema"
	starlarkextensions "github.com/rnovales/teleport/starlarkextensions"
	"github.com/ilyakaznacheev/cleanenv"
	log "github.com/sirupsen/logrus"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

var (
	// Databases contains the configuration for all databases
	Databases = make(map[string]Database)
)

type Database struct {
	URL      string
	Options  map[string]string
	Readonly bool
}

type DatabaseExtract struct {
	Source        string
	TableExtracts map[string]*TableExtract
}

type TableExtract struct {
	LoadOptions      LoadOptions
	ColumnTransforms map[string]ColumnTransform
	ComputedColumns  []ComputedColumn
}

type ComputedColumn struct {
	Name     string
	Type     string
	Function *starlark.Function
}

type ColumnTransform struct {
	Name      string
	Type      string
	Functions []*starlark.Function
}

type databasesConfig struct {
	Connections map[string]Database `yaml:"connections",json:"connections",toml:"connections",edn:"connections"`
}

func readDatabaseConnectionConfiguration() {
	files, err := readFiles(configDirectory)
	if err != nil {
		log.Warn(err)
	}
	for _, fileinfo := range files {
		if fileNameWithoutExtension(fileinfo.Name()) != "databases" {
			continue
		}

		config := databasesConfig{}
		err := cleanenv.ReadConfig(filepath.Join(workingDir(), configDirectory, fileinfo.Name()), &config)
		if err != nil {
			log.Fatal(err)
		}

		for key, database := range config.Connections {
			database.URL = os.ExpandEnv(database.URL)

			Databases[key] = database
		}
	}

	// Legacy connection configuration in ./databases
	if files, err = readFiles(legacyDatabasesConfigDirectory); err != nil {
		return
	}
	for _, fileinfo := range files {
		if strings.HasSuffix(fileinfo.Name(), ".port") {
			continue
		}

		var database Database
		err := cleanenv.ReadConfig(filepath.Join(workingDir(), legacyDatabasesConfigDirectory, fileinfo.Name()), &database)
		if err != nil {
			log.Fatal(err)
		}
		database.URL = os.ExpandEnv(database.URL)

		Databases[fileNameWithoutExtension(fileinfo.Name())] = database
	}
}

func readTableExtractConfiguration(path string, tableName string, tableExtractptr *TableExtract) error {
	databaseExtract := DatabaseExtract{}
	databaseExtract.TableExtracts = make(map[string]*TableExtract)

	portFile, err := findPortFile(path, []string{databasesConfigDirectory, legacyDatabasesConfigDirectory})
	if err != nil {
		log.Warn("No table configuration found, using default load strategy: Full")
		tableExtract := &TableExtract{}
		tableExtract.LoadOptions.Strategy = LoadStrategy("Full")
		*tableExtractptr = *tableExtract
		return nil
	}

	_, err = starlark.ExecFile(GetThread(), portFile, nil, databasePredeclared(&databaseExtract))
	if err != nil {
		return err
	}
	//
	// if err := tableExtract.validate(); err != nil {
	// 	return err
	// }

	var tableExtract *TableExtract
	tableExtract, ok := databaseExtract.TableExtracts[tableName]
	if !ok {
		tableExtract, ok = databaseExtract.TableExtracts["*"]

		if !ok {
			log.Warn("Missing extract configuration, assuming Full")
			tableExtract = &TableExtract{}
			tableExtract.LoadOptions.Strategy = LoadStrategy("Full")
		}
	}

	if FullLoad {
		tableExtract.LoadOptions.Strategy = LoadStrategy("Full")
	}

	if tableExtract.LoadOptions.Strategy == "" {
		tableExtract.LoadOptions.Strategy = defaultLoadStrategy
	}

	*tableExtractptr = *tableExtract
	return nil
}

// func (tableExtract *TableExtract) validate() error {
// 	validator.SetValidationFunc("in", validateIn)
// 	if errs := validator.Validate(tableExtract); errs != nil {
// 		return fmt.Errorf("Invalid Configuration: %s", errs.Error())
// 	}

// 	return nil
// }

func databasePredeclared(databaseExtract *DatabaseExtract) starlark.StringDict {
	predeclared := starlarkextensions.GetExtensions()
	// DSL Methods
	predeclared["Table"] = starlark.NewBuiltin("Table", databaseExtract.newTableExtract)

	// Load Strategies
	predeclared["Full"] = starlark.String(Full)
	predeclared["Incremental"] = starlark.String(Incremental)
	predeclared["ModifiedOnly"] = starlark.String(ModifiedOnly)

	return predeclared
}

func (databaseExtract *DatabaseExtract) newTableExtract(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (value starlark.Value, err error) {
	var (
		name starlark.String
	)
	if err := starlark.UnpackPositionalArgs("Table", args, kwargs, 1, &name); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	tableExtract := TableExtract{}
	databaseExtract.TableExtracts[name.GoString()] = &tableExtract
	members := make(starlark.StringDict, 2)
	members["LoadStrategy"] = starlark.NewBuiltin("LoadStrategy", tableExtract.setLoadStrategy)
	members["TransformColumn"] = starlark.NewBuiltin("LoadStrategy", tableExtract.addColumnTransform)
	members["ComputeColumn"] = starlark.NewBuiltin("LoadStrategy", tableExtract.addComputedColumn)
	sstruct := starlarkstruct.FromStringDict(name, members)
	GetThread().SetLocal("tableStruct", sstruct)
	return sstruct, nil
}

func (tableExtract *TableExtract) setLoadStrategy(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (value starlark.Value, err error) {
	var (
		strategy                     starlark.String
		primaryKey, ModifiedAtColumn starlark.String
		goBackHours                  starlark.Int
	)
	switch LoadStrategy(args[0].(starlark.String).GoString()) {
	case Full:
		if err := starlark.UnpackPositionalArgs("LoadStrategy", args, kwargs, 1, &strategy); err != nil {
			return nil, prependStarlarkPositionToError(thread, err)
		}
	case ModifiedOnly:
		if err := starlark.UnpackArgs("LoadStrategy", args, kwargs, "strategy", &strategy, "primary_key", &primaryKey, "modified_at_column", &ModifiedAtColumn, "go_back_hours", &goBackHours); err != nil {
			return nil, prependStarlarkPositionToError(thread, err)
		}
	case Incremental:
		if err := starlark.UnpackArgs("LoadStrategy", args, kwargs, "strategy", &strategy, "primary_key", &primaryKey); err != nil {
			return nil, prependStarlarkPositionToError(thread, err)
		}
	default:
		err := errors.New("LoadStrategy(): invalid strategy, allowed values: Full, ModifiedOnly, Incremental")
		return nil, prependStarlarkPositionToError(thread, err)
	}

	goBackHoursInt, err := strconv.Atoi(goBackHours.String())
	if err != nil {
		err := fmt.Errorf("LoadStrategy(): go_back_hours error: %w", err)
		return nil, prependStarlarkPositionToError(thread, err)
	}
	tableExtract.LoadOptions = LoadOptions{LoadStrategy(strategy), primaryKey.GoString(), ModifiedAtColumn.GoString(), goBackHoursInt}

	return GetThread().Local("tableStruct").(*starlarkstruct.Struct), nil
}

func (tableExtract *TableExtract) addColumnTransform(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (value starlark.Value, err error) {
	var (
		name       string
		function   *starlark.Function
		columnType string
	)

	if err := starlark.UnpackPositionalArgs("TransformColumn", args, kwargs, 2, &name, &function, &columnType); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	if tableExtract.ColumnTransforms == nil {
		tableExtract.ColumnTransforms = make(map[string]ColumnTransform)
	}
	if _, ok := tableExtract.ColumnTransforms[name]; !ok {
		tableExtract.ColumnTransforms[name] = ColumnTransform{Name: name}
	}
	columnTransform := tableExtract.ColumnTransforms[name]
	columnTransform.Type = columnType
	columnTransform.Functions = append(columnTransform.Functions, function)
	tableExtract.ColumnTransforms[name] = columnTransform

	return GetThread().Local("tableStruct").(*starlarkstruct.Struct), nil
}

func (tableExtract *TableExtract) addComputedColumn(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (value starlark.Value, err error) {
	var (
		name       string
		function   *starlark.Function
		columnType string
	)

	if err := starlark.UnpackPositionalArgs("ComputeColumn", args, kwargs, 2, &name, &function, &columnType); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	if columnType == "" {
		columnType = "TEXT"
	}

	tableExtract.ComputedColumns = append(tableExtract.ComputedColumns, ComputedColumn{name, columnType, function})

	return GetThread().Local("tableStruct").(*starlarkstruct.Struct), nil
}

func (tableExtract *TableExtract) strategyOpts() (strategyOpts StrategyOptions) {
	strategyOpts.Strategy = string(tableExtract.LoadOptions.Strategy)
	strategyOpts.PrimaryKey = tableExtract.LoadOptions.PrimaryKey
	strategyOpts.ModifiedAtColumn = tableExtract.LoadOptions.ModifiedAtColumn
	strategyOpts.HoursAgo = fmt.Sprintf("%d", tableExtract.LoadOptions.GoBackHours)
	return
}

func (computedColumn *ComputedColumn) toColumn() (column schema.Column, err error) {
	dataType, options, err := schema.GenericDatabase.ParseDatabaseTypeFromString(computedColumn.Type)
	if err != nil {
		return
	}

	column.Name = computedColumn.Name
	column.DataType = dataType
	column.Options = options
	column.Options[schema.COMPUTED] = 1

	return
}

func databaseSchema(source string, driver string) (string, bool) {
	if schema, ok := Databases[source].Options["schema"]; ok {
		return schema, true
	} else if driver == "snowflake" {
		return "PUBLIC", true
	}

	return "", false
}
