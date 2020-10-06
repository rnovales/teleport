package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/rnovales/teleport/secrets"
	log "github.com/sirupsen/logrus"
)

type CliOptions struct {
	Command    string
	Source     string
	FromSource string
	ToSource   string
	TableName  string
	File       string
	Preview    bool
	Debug      bool
	FullLoad   bool
}

type StrategyOptions struct {
	Strategy         string
	PrimaryKey       string
	ModifiedAtColumn string
	HoursAgo         string
}

func parseArguments() CliOptions {
	options := CliOptions{}

	flag.Usage = help
	options.Command = os.Args[1]
	flag.StringVar(&options.Source, "source", "", "Name of data source to perform command on")
	flag.StringVar(&options.Source, "s", "", "Alias for -source")
	flag.StringVar(&options.FromSource, "from", "", "Data source to extract data from")
	flag.StringVar(&options.ToSource, "to", "", "Data source to load data into")
	flag.StringVar(&options.TableName, "table", "", "Name of table to perform operations on")
	flag.StringVar(&options.TableName, "t", "", "Alias for -table")
	flag.StringVar(&options.File, "file", "", "Path to file to be used in command")
	flag.StringVar(&options.File, "f", "", "Alias for -file")
	flag.BoolVar(&options.FullLoad, "full", options.Debug, "override Load Strategy configuration with the Full strategy")
	flag.BoolVar(&options.Preview, "p", false, "alias for -preview")
	flag.BoolVar(&options.Preview, "preview", options.Preview, "use preview mode to perform a dry-run with truncated data and verbose logging")
	flag.BoolVar(&options.Debug, "d", false, "alias for -debug")
	flag.BoolVar(&options.Debug, "debug", options.Debug, "enable debug log messages")

	flag.CommandLine.Parse(os.Args[2:])

	return options
}

func help() {
	fmt.Println("usage: teleport [COMMAND] [OPTIONS]")
	fmt.Println("Commands:")
	fmt.Println("  new <path/to/pad>\tgenerate a new pad folder at the given path")
	fmt.Println("  secrets <command>\tmanage encrypted ENV variables for your pad")
	fmt.Println("  schedule <command>\tmanage the job schedule configuration for your pad")
	fmt.Println("  help\t\t\tshow this message")
	fmt.Println("  version\t\tprint version information")
	fmt.Println("")
	fmt.Println("  extract-db\t\texport data from a database table to CSV. Required options: -from, -table")
	fmt.Println("  extract-api\t\texport data from an API endpoint to CSV. Required options: -from")
	fmt.Println("")
	fmt.Println("  extract-load-db\t\texport data from a table in one database to another. Required options: -from, -to, -table")
	fmt.Println("  extract-load-api\t\texport data from an API endpoint to a database. Required options: -from, -to")
	fmt.Println("")
	fmt.Println("  transform\t\t(re-)generate a materialized table form a sql statement. Required options: -source, -table")
	fmt.Println("")
	fmt.Println("  about-db\t\tshow connection information a database. Required options: -source")
	fmt.Println("  db-terminal\t\tstart a terminal for interacting with a database. Required options: -source")
	fmt.Println("  list-tables\t\tlist the tables in a database. Required options: -source")
	fmt.Println("  drop-table\t\tdrop a table. Required options: -source, -table")
	fmt.Println("  describe-table\tprint the schema for a table. Required options: -source, -table")
	fmt.Println("")
	fmt.Println("Options:")
	fmt.Println("  -source, -s [source]\tdata source name")
	fmt.Println("  -from [source]\tdata source to extract data from")
	fmt.Println("  -to [source]\t\tdata source to load data into")
	fmt.Println("  -table, -t [table]\tname of table in the database data source")
	fmt.Println("  -preview, -p\t\tpreview command as a dry-run without making any changes")
	fmt.Println("  -debug, -d\t\tenable debug log output")
	fmt.Println("  -full\t\t\toverride the configured load strategy and use Full instead")
}

func version() {
	fmt.Fprintln(os.Stderr, fmt.Sprintf("Teleport %s (build: %s)", Version, Build))
}

func listCommands() {
	fmt.Println("")
	fmt.Println("Teleport commands")
	fmt.Println("new\tsecrets\thelp\tversion")
	fmt.Println("")
	fmt.Println("Extract commands")
	fmt.Println("extract-db\textract-api")
	fmt.Println("")
	fmt.Println("Extract and load commands")
	fmt.Println("extract-load-db\textract-load-api")
	fmt.Println("")
	fmt.Println("Transform commands")
	fmt.Println("transform")
	fmt.Println("")
	fmt.Println("Database commands")
	fmt.Println("about-db\tdb-terminal\tlist-tables")
	fmt.Println("drop-table\tdescribe-table")
}

func secretsCLI() {
	if len(os.Args) < 3 {
		secretsHelp()
		return
	}

	switch os.Args[2] {
	case "generate_secret_key":
		secretKey, err := secrets.GenerateSecretKey()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("# Store this ENV value somewhere safe, do not commit it to the repository in plain text.")
		fmt.Println("# This ENV value must be set in all environments that will need to use the decrypted secrets.")
		fmt.Println()
		fmt.Printf("%s=%s\n", "TELEPORT_SECRET_KEY", secretKey)
	case "init":
		err := secrets.InitializeSecretsFile(secretsSettings())
		if err != nil {
			if err.Error() == "TELEPORT_SECRET_KEY not set" {
				log.Fatal(fmt.Errorf("%s, use `teleport secrets generate_secret_key` to create the secret key", err))
			} else {
				log.Fatal(err)
			}
		}
	case "show":
		body, err := secrets.ReadSecretsFile(secretsSettings())
		if err != nil {
			log.Fatal(err)
		}

		for _, variable := range body {
			fmt.Println(variable.ToEnvFormat())
		}
	case "set":
		var (
			key   string
			value string
		)
		switch len(os.Args) {
		case 4:
			key = os.Args[3]
			value, err := promptForValue()
			if err != nil {
				log.Fatal(err)
			}

			err = secrets.UpdateSecret(secretsSettings(), key, value)
			if err != nil {
				log.Fatal(err)
			}
		case 5:
			key = os.Args[3]
			value = os.Args[4]

			err := secrets.UpdateSecret(secretsSettings(), key, value)
			if err != nil {
				log.Fatal(err)
			}
		default:
			fmt.Println("Wrong number of options provided to `teleport secrets set`")
			fmt.Println("Syntax:")
			fmt.Println("  teleport secrets set [KEY] <[VALUE]>")
		}
	case "delete":
		if len(os.Args) != 4 {
			fmt.Println("Wrong number of options provided to `teleport secrets delete`")
			fmt.Println("Syntax:")
			fmt.Println("  teleport secrets delete [KEY]")
			return
		}
		err := secrets.DeleteSecret(secretsSettings(), os.Args[3])
		if err != nil {
			log.Fatal(err)
		}
	default:
		secretsHelp()
	}
}

func scheduleCLI() {
	if len(os.Args) != 3 {
		scheduleHelp()
		return
	}

	switch os.Args[2] {
	case "validate":
		if err := readSchedule(); err != nil {
			log.Fatal(err)
		}
		log.Infof("%d valid items in schedule ✓", len(schedule))
		return
	case "export":
		if err := readSchedule(); err != nil {
			log.Fatal(err)
		}
		if bytes, err := exportSchedule(); err != nil {
			log.Fatal(err)
		} else {
			fmt.Println(string(bytes))
			return
		}
	default:
		scheduleHelp()
	}
}

func pluginsCLI() {
	if len(os.Args) < 3 {
		pluginsHelp()
		return
	}

	switch os.Args[2] {
	case "install":
		if len(os.Args) < 4 {
			pluginsHelp()
			return
		}

		err := InstallPlugin(os.Args[3])
		if err != nil {
			log.Fatal(err)
		}
	case "--":
		err := CallPlugin(os.Args[3], os.Args[4:])
		if err != nil {
			log.Fatal(err)
		}
	default:
		pluginsHelp()
	}
}

func secretsHelp() {
	fmt.Println("usage: teleport secrets [COMMAND] <[ARGS]...>")
	fmt.Println("Commands:")
	fmt.Println("  generate_secret_key\tgenerate and print a random string to use as the encryption secret key")
	fmt.Println("  init\t\t\tadd an empty encrypted secrets file to the current Pad directory")
	fmt.Println("  show\t\t\tdecrypt and print all the items in the secrets file")
	fmt.Println("  set [KEY] <[VALUE]>\tcreate or update a secret by key; password prompt will be used if VALUE is not provided in the command")
	fmt.Println("  delete [KEY]\t\tdelete a secret by key")
}

func scheduleHelp() {
	fmt.Println("usage: teleport schedule [COMMAND]")
	fmt.Println("Commands:")
	fmt.Println("  validate\treads the schedule file and prints any errors")
	fmt.Println("  export\texport the schedule as JSON for handling by infrastructure")
}

func pluginsHelp() {
	fmt.Println("usage: teleport plugin [COMMAND] [PLUGIN_NAME] <[ARGS]...>")
	fmt.Println("Commands:")
	fmt.Println("  install [PLUGIN_NAME]\t\tinstall PLUGIN_NAME to the current Pad directory")
	fmt.Println("  -- [PLUGIN_NAME] <[ARGS]...>\trun the plugin with provided arguments")
}

func promptForValue() (string, error) {
	fmt.Print("Value: ")

	attrs := syscall.ProcAttr{
		Dir:   "",
		Env:   []string{},
		Files: []uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd()},
		Sys:   nil}
	var ws syscall.WaitStatus

	pid, err := syscall.ForkExec(
		"/bin/stty",
		[]string{"stty", "-echo"},
		&attrs)
	if err != nil {
		return "", err
	}

	_, err = syscall.Wait4(pid, &ws, 0, nil)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(os.Stdin)
	value, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	value = strings.TrimSuffix(value, "\n")

	pid, err = syscall.ForkExec(
		"/bin/stty",
		[]string{"stty", "echo"},
		&attrs)
	if err != nil {
		return "", err
	}

	return value, nil
}
