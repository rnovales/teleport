package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	slutil "github.com/hundredwatt/starlib/util"
	"github.com/rnovales/teleport/schema"
	log "github.com/sirupsen/logrus"
	"go.starlark.net/starlark"
)

type dataObject struct {
	headers []string
	values  []interface{}
}

type table [][]string

type apiError struct {
	class errorClass
	err   error
}

type errorClass string

const (
	NetworkError     errorClass = "NetworkError"
	Http4XXError     errorClass = "Http4XXError"
	Http5XXError     errorClass = "Http5XXError"
	InvalidBodyError errorClass = "InvalidBodyError"
)

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.class, e.err.Error())
}

func extractLoadAPI(endpointName string, destination string) {
	fnlog := log.WithFields(log.Fields{
		"from": endpointName,
		"to":   destination,
	})
	fnlog.Info("Starting extract-load-api")

	var endpoint Endpoint
	var destinationTable schema.Table
	var columns []schema.Column
	var results []dataObject
	var csvfile string

	destinationTableName := strings.TrimSuffix(filepath.Base(endpointName), filepath.Ext(endpointName))

	RunWorkflow([]func() error{
		func() error { return readEndpointConfiguration(endpointName, &endpoint) },
		func() error { return connectDatabaseWithLogging(destination) },
		func() error {
			return createEndpointdestinationTableIfNotExists(destination, destinationTableName, &endpoint)
		},
		func() error { return inspectTable(destination, destinationTableName, &destinationTable, nil) },
		func() error { return performAPIExtraction(&endpoint, &results) },
		func() error { return determineImportColumns(&destinationTable, results, &columns) },
		func() error { return saveResultsToCSV(endpointName, results, &columns, &csvfile, false) },
		func() error { return load(&destinationTable, &columns, &csvfile, endpoint.strategyOpts()) },
	}, func() {
		fnlog.WithField("rows", currentWorkflow.RowCounter).Info("Completed extract-load-api 🎉")
	})
}

func extractAPI(endpointName string) {
	destinationTableName := strings.TrimSuffix(filepath.Base(endpointName), filepath.Ext(endpointName))

	log.WithFields(log.Fields{
		"from": destinationTableName,
	}).Info("Starting extract-api")

	var endpoint Endpoint
	var results []dataObject
	var csvfile string

	RunWorkflow([]func() error{
		func() error { return readEndpointConfiguration(endpointName, &endpoint) },
		func() error { return performAPIExtraction(&endpoint, &results) },
		func() error { return saveResultsToCSV(endpointName, results, nil, &csvfile, true) },
	}, func() {
		log.WithFields(log.Fields{
			"file": csvfile,
			"rows": currentWorkflow.RowCounter,
		}).Info("Extract to CSV completed 🎉")
	})
}

func createEndpointdestinationTableIfNotExists(destination string, destinationTableName string, endpoint *Endpoint) (err error) {
	fnlog := log.WithFields(log.Fields{
		"database": destination,
		"table":    destinationTableName,
	})

	exists, err := tableExists(destination, destinationTableName)
	if err != nil {
		return
	} else if exists {
		return
	} else if endpoint.TableDefinition == nil {
		return
	}

	db, err := connectDatabase(destination)
	if err != nil {
		return
	}

	statement := fmt.Sprintf("CREATE TABLE %s (\n", destinationTableName)
	for name, datatype := range *endpoint.TableDefinition {
		statement = statement + fmt.Sprintf("\t%s %s,\n", name, datatype)
	}
	statement = strings.TrimSuffix(statement, ",\n")
	statement += "\n)"

	fnlog.Infof("Destination Table does not exist, creating")
	// if Preview {
	// log.Debug("(not executed) SQL Query:\n" + indentString(statement))
	// return
	// }

	_, err = db.Exec(statement)

	return
}

func determineImportColumns(destinationTable *schema.Table, results []dataObject, columns *[]schema.Column) error {
	headers := results[0].headers

	importColumns := make([]schema.Column, 0)
	for _, column := range destinationTable.Columns {
		for _, header := range headers {
			if column.Name == header {
				importColumns = append(importColumns, column)
				break
			}
		}
	}

	if len(importColumns) == 0 {
		return errors.New("extracted results and destination table have no columns in common")
	}

	*columns = importColumns

	return nil
}

func performAPIExtraction(endpoint *Endpoint, finalResults *[]dataObject) error {
	originalErr := requestAllPages(endpoint, finalResults)
	if originalErr == nil {
		return nil
	}

	if _, ok := originalErr.(*apiError); !ok {
		return originalErr
	} else if endpoint.ErrorHandling == nil {
		return originalErr
	}

	return handleAPIError(originalErr.(*apiError), endpoint.ErrorHandling)
}

func requestAllPages(endpoint *Endpoint, finalResults *[]dataObject) error {
	baseURL := endpoint.URL
	results := make([]dataObject, 0)

	var resp *http.Response
	var unmarshalledBody interface{}
	var itr int = 0
	for {
		pageLog := log.WithField("page", itr)

		pagination, stop, err := updatePagination(resp, unmarshalledBody, endpoint)
		if err != nil {
			return err
		}

		if stop && itr != 0 {
			break
		}

		currentURL := baseURL
		for k, v := range pagination {
			pageLog = pageLog.WithField(k, v)
			token := fmt.Sprintf("{%s}", k)
			currentURL = strings.NewReplacer(token, v).Replace(currentURL)
		}

		pageLog.Debug("Requesting page")

		resp, err = getResponse(endpoint.Method, currentURL, endpoint.Headers, endpoint.BasicAuth)
		if err != nil {
			return &apiError{NetworkError, fmt.Errorf("http response error: %w", err)}
		}

		switch sc := resp.StatusCode; {
		case sc >= 500:
			err := fmt.Errorf("%s: %s", resp.Status, resp.Body)
			return &apiError{Http5XXError, err}
		case sc >= 400:
			err := fmt.Errorf("%s: %s", resp.Status, resp.Body)
			return &apiError{Http4XXError, err}
		}

		unmarshalledBody, err = unmarshalBody(endpoint.ResponseType, resp.Body)
		if err != nil {
			return &apiError{InvalidBodyError, err}
		}

		pageResults, err := applyTransform(unmarshalledBody, endpoint)
		if err != nil {
			return err
		}
		results = append(results, pageResults...)

		if Preview && itr != 0 {
			if len(results) > PreviewLimit {
				results = results[:PreviewLimit]
			}
			log.Debug("(preview) Skipping additional pages if any")
			break
		}

		itr++
	}

	*finalResults = results

	return nil
}

func getResponse(method string, url string, headers map[string]string, basicAuth *map[string]string) (resp *http.Response, err error) {
	client := &http.Client{}

	req, err := http.NewRequest(strings.ToUpper(method), url, nil)
	if err != nil {
		return
	}

	req.Header.Set("User-Agent", "Teleport")

	for key, value := range headers {
		req.Header.Add(key, value)
	}

	if basicAuth != nil {
		req.SetBasicAuth((*basicAuth)["username"], (*basicAuth)["password"])
	}

	resp, err = client.Do(req)
	if err != nil {
		err = fmt.Errorf("http error: %w", err)
		return
	}

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("body read error: %w", err)
		return
	}
	rdr1 := ioutil.NopCloser(bytes.NewBuffer(buf))
	rdr2 := ioutil.NopCloser(bytes.NewBuffer(buf))
	resp.Body = rdr1

	if err != nil {
		log.Errorf("HTTP Response Body: %.500q", rdr2)
	} else if log.GetLevel() == log.DebugLevel {
		log.Debugf("HTTP Response Body: %.500q", rdr2)
	}

	return
}

func applyTransform(value interface{}, endpoint *Endpoint) (results []dataObject, err error) {
	if endpoint.Transform != nil || endpoint.Functions["Transform"] != nil {
		switch value.(type) {
		case starlark.Value:
		default:
			value, err = slutil.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("starlib marshall error: %w", err)
			}
		}

		if endpoint.Transform != nil {
			value, err = starlark.Call(GetThread(), endpoint.Transform, starlark.Tuple{value.(starlark.Value)}, nil)
			if err != nil {
				return nil, appendBackTraceToStarlarkError(err)
			}
		} else {
			value, err = starlark.Call(GetThread(), endpoint.Functions["Transform"], starlark.Tuple{value.(starlark.Value)}, nil)
			if err != nil {
				return nil, appendBackTraceToStarlarkError(err)
			}

		}

		switch value {
		case starlark.None:
			return nil, fmt.Errorf("Transform() error: no return statement or None returned. To return no results, use `return []`")
		}
	}

	switch value.(type) {
	case *starlark.List:
		objectItr := value.(*starlark.List).Iterate()
		var slobject starlark.Value
		defer objectItr.Done()
		for objectItr.Next(&slobject) {
			object, err := starlarkUnmarshal(slobject)
			if err != nil {
				return nil, fmt.Errorf("read object error: %w", err)
			}

			IncrementRowCounter()
			results = append(results, object.(dataObject))
		}
	case *starlark.Dict:
		fmt.Println(value.(*starlark.Dict).Keys())
		object, err := starlarkUnmarshal(value.(starlark.Value))
		fmt.Println(object)
		if err != nil {
			return nil, fmt.Errorf("read object error: %w", err)
		}
		IncrementRowCounter()
		results = append(results, object.(dataObject))
	case []interface{}:
		for _, object := range value.([]interface{}) {
			data := newDataObject(len(object.(map[interface{}]interface{})))
			i := 0
			for k, v := range object.(map[interface{}]interface{}) {
				data.headers[i] = k.(string)
				data.values[i] = v
				i++
			}
			data.sortKeys()
			IncrementRowCounter()
			results = append(results, data)
		}
	case map[interface{}]interface{}:
		data := newDataObject(len(value.(map[interface{}]interface{})))
		i := 0
		for k, v := range value.(map[interface{}]interface{}) {
			data.headers[i] = k.(string)
			data.values[i] = v
			i++
		}
		data.sortKeys()
		IncrementRowCounter()
		results = append(results, data)
	default:
		return nil, fmt.Errorf("unsupported parser return type: %T", value)
	}

	return
}

func updatePagination(response *http.Response, body interface{}, endpoint *Endpoint) (map[string]string, bool, error) {
	null := make(map[string]string)
	if endpoint.Paginate == nil && endpoint.Functions["Paginate"] == nil {
		return null, true, nil
	}

	var args starlark.Tuple
	if response != nil {
		headers := make(map[string]interface{}) // starlib.Marshall doesn't support map[string]string
		for k, v := range response.Header {
			headers[k] = v[0]
		}
		previousResponse := map[string]interface{}{
			"body":    body,
			"headers": headers,
		}
		marshalled, err := slutil.Marshal(previousResponse)
		if err != nil {
			return null, true, fmt.Errorf("starlib marshall error: %w", err)
		}
		args = starlark.Tuple{marshalled}
	} else {
		args = starlark.Tuple{starlark.None}
	}

	var result starlark.Value
	var err error
	if endpoint.Paginate != nil {
		result, err = starlark.Call(GetThread(), endpoint.Paginate, args, nil)
		if err != nil {
			return null, true, fmt.Errorf("Paginate() error: %w", appendBackTraceToStarlarkError(err))
		}
	} else {
		result, err = starlark.Call(GetThread(), endpoint.Functions["Paginate"], args, nil)
		if err != nil {
			return null, true, fmt.Errorf("Paginate() error: %w", appendBackTraceToStarlarkError(err))
		}
	}

	switch result.(type) {
	case *starlark.Dict:
		unmarshalled, err := slutil.Unmarshal(result.(starlark.Value))
		if err != nil {
			return null, true, fmt.Errorf("Paginate() result object error: %w", err)
		}
		pagination := make(map[string]string)
		for k, v := range unmarshalled.(map[string]interface{}) {
			pagination[k] = fmt.Sprintf("%v", v)
		}
		return pagination, false, nil
	case starlark.NoneType, nil:
		return null, true, nil
	default:
		return null, true, fmt.Errorf("Paginate() returned unsupported value: %q", result)
	}
}

func saveResultsToCSV(endpointName string, results []dataObject, columns *[]schema.Column, csvfile *string, includeHeaders bool) error {
	headers := make([]string, 0)
	if columns == nil {
		headers = results[0].headers
	} else {
		for _, column := range *columns {
			headers = append(headers, column.Name)
		}
	}

	name := fmt.Sprintf("extract-api-%s-*.csv", strings.TrimSuffix(filepath.Base(endpointName), filepath.Ext(endpointName)))

	filename, err := generateCSV(headers, name, includeHeaders, func(writer *csv.Writer) error {
		writeBuffer := make([]string, len(headers))

		for _, object := range results {
			for i, key := range headers {
				writeBuffer[i] = formatForCSV(object.get(key))
			}

			err := writer.Write(writeBuffer)
			if err != nil {
				return err
			}
		}

		return nil
	})
	*csvfile = filename

	return err
}

func unmarshalBody(responseType string, raw io.ReadCloser) (output interface{}, err error) {
	switch responseType {
	case "json":
		output, err = applyJSONTransform(raw)
	case "csv":
		reader := csv.NewReader(raw)
		records, csverr := reader.ReadAll()
		if csverr != nil {
			err = csverr
			return
		}
		output = table(records)
	default:
		err = errors.New("unsupported response type. Allowed values: json")
	}
	return
}

func handleAPIError(err *apiError, errorHandling *map[errorClass]ExitCode) error {
	value, ok := (*errorHandling)[err.class]
	if !ok {
		return WorkflowFail(err)
	}

	switch value {
	case Fail:
		return WorkflowFail(err)
	case Retry:
		return WorkflowRetry(err)
	default:
		return WorkflowFail(err)
	}
}

func applyJSONTransform(raw io.ReadCloser) (output interface{}, err error) {
	decoder := json.NewDecoder(raw)
	decoder.UseNumber()
	err = decoder.Decode(&output)
	if err != nil {
		return nil, fmt.Errorf("json decode error: %w", err)
	}

	output, err = convertJSONNumbers(output)
	if err != nil {
		return nil, fmt.Errorf("json convert number error: %w", err)
	}

	return
}

func convertJSONNumbers(data interface{}) (v interface{}, err error) {
	switch x := data.(type) {
	case json.Number:
		// TODO: need to workthrough starlark's issues with 64bit ints before we can enable this
		// if int, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
		// v = int
		// } else {
		// v = x.String()
		// }
		v = x.String()
	case []interface{}:
		var elems = make([]interface{}, len(x))
		for i, val := range x {
			elems[i], err = convertJSONNumbers(val)
			if err != nil {
				return
			}
		}
		v = elems
	case map[interface{}]interface{}:
		dict := make(map[interface{}]interface{})
		var elem interface{}
		for key, val := range x {
			elem, err = convertJSONNumbers(val)
			if err != nil {
				return
			}
			dict[key] = elem
		}
		v = dict
	case map[string]interface{}:
		dict := make(map[interface{}]interface{})
		var elem interface{}
		for key, val := range x {
			elem, err = convertJSONNumbers(val)
			if err != nil {
				return
			}
			dict[key] = elem
		}
		v = dict
	default:
		v = x
	}
	return
}

func starlarkUnmarshal(x starlark.Value) (val interface{}, err error) {
	switch v := x.(type) {
	case *starlark.Dict:
		var (
			dictVal starlark.Value
			pval    interface{}
			kval    interface{}
			keys    []interface{}
			vals    []interface{}
			// key as interface if found one key is not a string
			ki bool
		)

		for _, k := range v.Keys() {
			dictVal, _, err = v.Get(k)
			if err != nil {
				return
			}

			pval, err = slutil.Unmarshal(dictVal)
			if err != nil {
				err = fmt.Errorf("unmarshaling starlark value: %w", err)
				return
			}

			kval, err = slutil.Unmarshal(k)
			if err != nil {
				err = fmt.Errorf("unmarshaling starlark key: %w", err)
				return
			}

			if _, ok := kval.(string); !ok {
				// found key as not a string
				ki = true
			}

			keys = append(keys, kval)
			vals = append(vals, pval)
		}

		// prepare result

		rs := newDataObject(len(keys))
		ri := map[interface{}]interface{}{}

		for i, key := range keys {
			// key as interface
			if ki {
				ri[key] = vals[i]
			} else {
				rs.headers[i] = key.(string)
				rs.values[i] = vals[i]
			}
		}

		if ki {
			val = ri // map[interface{}]interface{}
		} else {
			val = rs // map[string]interface{}
		}
	default:
		val, err = slutil.Unmarshal(v)
	}

	return
}

func newDataObject(size int) dataObject {
	return dataObject{
		headers: make([]string, size),
		values:  make([]interface{}, size),
	}
}

func (object dataObject) get(key string) interface{} {
	for i, k := range object.headers {
		if k == key {
			return object.values[i]
		}
	}

	return nil
}

func (object dataObject) sortKeys() {
	sorted := newDataObject(len(object.headers))
	copy(sorted.headers, object.headers)
	sort.Strings(sorted.headers)
	for i, k := range sorted.headers {
		sorted.values[i] = object.get(k)
	}

	copy(object.headers, sorted.headers)
	copy(object.values, sorted.values)
}

func (t table) MarshalStarlark() (v starlark.Value, err error) {
	var itable = make([]interface{}, len(t))
	for i, row := range t {
		var irow = make([]interface{}, len(row))
		for i := range row {
			irow[i] = row[i]
		}
		itable[i] = irow
	}
	v, err = slutil.Marshal(itable)

	return
}
