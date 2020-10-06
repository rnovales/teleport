package main

import (
	"fmt"
	"strings"

	"github.com/rnovales/teleport/schema"
	log "github.com/sirupsen/logrus"
)

func importSnowflake(db *schema.Database, table string, file string, columns []schema.Column, options map[string]string) error {
	log.Debug("Uploading CSV to S3")
	s3URL, err := uploadFileToS3(options["s3_bucket_region"], options["s3_bucket"], file)
	if err != nil {
		return fmt.Errorf("s3 upload error: %w", err)
	}
	path := strings.TrimPrefix(s3URL, fmt.Sprintf("s3://%s/", options["s3_bucket"]))

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	log.Debug("Executing Snowflake COPY command")
	q := fmt.Sprintf(`
		COPY INTO %s
		FROM @%s/%s
		file_format = ( TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY = '"', REPLACE_INVALID_CHARACTERS = True, EMPTY_FIELD_AS_NULL = True )
	`, db.EscapeIdentifier(table), options["external_stage_name"], path)

	_, err = db.Exec(q)

	if err != nil {
		return fmt.Errorf("snowflake copy error: %w", err)
	}

	return nil
}
