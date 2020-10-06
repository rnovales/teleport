package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rnovales/teleport/schema"
	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
)

func TestAPIConfigurationCases(t *testing.T) {
	cases := []struct {
		testAPIFile             string
		expectedLastEntryLevel  log.Level
		expectedRows            int64
		expectLastEntryContains []string
	}{
		{
			"api_basic_auth.port",
			log.InfoLevel,
			2,
			[]string{},
		},
		{
			"api_header_auth.port",
			log.InfoLevel,
			2,
			[]string{},
		},
		{
			"api_csv.port",
			log.InfoLevel,
			10,
			[]string{},
		},
		{
			"api_offset_pagination.port",
			log.InfoLevel,
			4,
			[]string{},
		},
		{
			"api_integer_data_type.port",
			log.InfoLevel,
			2,
			[]string{},
		},
		{
			"api_lambda_transform.port",
			log.InfoLevel,
			10,
			[]string{},
		},
		{
			"api_invalid_configuration.port",
			log.FatalLevel,
			-1,
			[]string{"URL: regular expression mismatch", "ResponseType: value 'glorb' not allowed"},
		},
		{
			"api_missing_return_value.port",
			log.FatalLevel,
			-1,
			[]string{"Transform() error: no return statement"},
		},
		{
			"api_invalid_body.port",
			log.FatalLevel,
			-1,
			[]string{"InvalidBodyError: json decode error"},
		},
		{
			"api_500.port",
			log.FatalLevel,
			-1,
			[]string{"Http5XXError: 500 Internal Server Error"},
		},
	}

	for _, cse := range cases {
		t.Run(cse.testAPIFile, func(t *testing.T) {
			runAPITest(t, cse.testAPIFile, func(t *testing.T, portFile string, destdb *schema.Database) {
				extractAPI(portFile)

				assert.Equal(t, cse.expectedLastEntryLevel, hook.LastEntry().Level)
				if cse.expectedRows != -1 {
					assert.Equal(t, cse.expectedRows, hook.LastEntry().Data["rows"])
				}
				for _, contains := range cse.expectLastEntryContains {
					lastString, err := hook.LastEntry().String()
					assert.NoError(t, err)
					assert.Contains(t, lastString, contains)
				}
			})
		})
	}
}

func TestAPIHeadersCases(t *testing.T) {
	cases := []struct {
		testAPIFile     string
		expectedHeaders []string
	}{
		{
			"api_basic_auth.port",
			[]string{"id", "name", "created_at"},
		},
		{
			"api_csv.port",
			[]string{"id", "price", "ranking", "name", "active", "launched", "created_at", "description"},
		},
		{
			"api_no_transform.port",
			[]string{"items", "offset"}, // should be alphabetically sorted
		},
	}

	for _, cse := range cases {
		t.Run(cse.testAPIFile, func(t *testing.T) {
			runAPITest(t, cse.testAPIFile, func(t *testing.T, portFile string, destdb *schema.Database) {
				extractAPI(portFile)

				for i, header := range cse.expectedHeaders {
					assertCsvCellContents(t, header, hook.LastEntry().Data["file"].(string), 0, i)
				}
			})
		})
	}
}

func TestAPIPreview(t *testing.T) {
	Preview = true
	runAPITest(t, "api_offset_pagination.port", func(t *testing.T, portFile string, destdb *schema.Database) {
		extractLoadAPI(portFile, "testdest")

		assertRowCount(t, 0, destdb, "test_items")
		twelvthString, _ := hook.Entries[12].String()
		assert.Contains(t, twelvthString, "(not executed)")
	})
	Preview = false
}

func TestIncrementalLoadStrategy(t *testing.T) {
	runAPITest(t, "api_incremental_load_strategy.port", func(t *testing.T, portFile string, destdb *schema.Database) {
		destdb.Exec(`CREATE TABLE test_items (id INT, name VARCHAR(255));`)
		destdb.Exec(`INSERT INTO test_items (id, name) VALUES (9, "Bono");`)

		extractLoadAPI(portFile, "testdest")
		assertRowCount(t, 3, destdb, "test_items")
	})
}

func runAPITest(t *testing.T, testfile string, testfn func(*testing.T, string, *schema.Database)) {
	ts := testServer()

	os.Setenv("TEST_URL", ts.URL)
	defer os.Unsetenv("TEST_URL")

	bytes, err := ioutil.ReadFile(filepath.Join("testdata/apis", testfile))
	if err != nil {
		t.Fatal(err)
	}
	configuration := string(bytes)

	tmpFile, err := ioutil.TempFile(os.TempDir(), "test_items.*port")
	if err != nil {
		t.Fatal("cannot create temporary file:", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err = tmpFile.WriteString(configuration); err != nil {
		t.Fatal("failed to write to temporary file:", err)
	}

	Databases["testdest"] = Database{"sqlite://:memory:", map[string]string{}, false}
	db, _ := connectDatabase("testdest")
	defer delete(dbs, "testdest")

	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	defer log.SetLevel(log.GetLevel())
	log.SetLevel(log.DebugLevel)

	log.StandardLogger().ExitFunc = func(int) {}

	hook.Reset()

	testfn(t, tmpFile.Name(), db)

	for _, entry := range hook.Entries {
		t.Log(entry.String())
	}
}

func testServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Supports either Basic or Bearer authentication
		username, password, ok := r.BasicAuth()
		authorization := r.Header.Values("Authorization")
		if ok && username == "user" && password == "pass" {
			// authenticated!
		} else if len(authorization) == 1 && authorization[0] == "Bearer 292b0e" {
			// authenticated!
		} else {
			w.WriteHeader(401)
			return
		}

		switch r.URL.Path {
		case "/500":
			w.WriteHeader(500)
		case "/text.txt":
			fmt.Fprintln(w, "Hello, world!")
		case "/widgets.csv":
			headerrow := "id,price,ranking,name,active,launched,created_at,description"
			bytes, _ := ioutil.ReadFile("testdata/example_widgets.csv")
			body := string(bytes)
			csv := strings.Join([]string{headerrow, body}, "\n")

			fmt.Fprintln(w, csv)
		case "/", "/items.json":
			// Supports Pagination
			var offset string
			switch len(r.URL.Query()["offset"]) {
			case 0:
				offset = ""
			case 1:
				offset = r.URL.Query()["offset"][0]
			}

			switch offset {
			case "", "0":
				items := `[{"id":1,"name":"Santana","created_at":1590870032},{"id":2,"name":"David Grohl","created_at":1585599636}]`
				fmt.Fprintln(w, fmt.Sprintf(`{"items":%s, "offset": 2}`, items))
			case "2":
				items := `[{"id":3,"name":"Jimmy Hendrix","created_at":1585873398},{"id":4,"name":"Travis Barker","created_at":1588033399}]`
				fmt.Fprintln(w, fmt.Sprintf(`{"items":%s, "offset": null}`, items))
			}
		}
	}))
}
