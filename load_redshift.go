package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rnovales/teleport/schema"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func importRedshift(db *schema.Database, table string, file string, columns []schema.Column, options map[string]string) error {
	log.Debug("Uploading CSV to S3")
	s3URL, err := uploadFileToS3(options["s3_bucket_region"], options["s3_bucket"], file)
	if err != nil {
		return fmt.Errorf("s3 upload error: %w", err)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	copyOptions := ""
	if options["s3_bucket_region"] != "" {
		copyOptions += fmt.Sprintf("REGION '%s'", options["s3_bucket_region"])
	}

	csvOptions := "EMPTYASNULL ACCEPTINVCHARS TIMEFORMAT 'auto'"

	log.Debug("Executing Redshift COPY command")
	_, err = db.Exec(fmt.Sprintf(`
		COPY %s
		(%s)
		FROM '%s'
		IAM_ROLE '%s'
		%s
		CSV
		%s;
	`, table, strings.Join(columnNames, ", "), s3URL, options["service_role"], copyOptions, csvOptions))

	if err != nil {
		return fmt.Errorf("redshift copy error: %w", err)
	}

	return nil
}

func uploadFileToS3(region string, bucket string, filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}

	keyElements := []string{"teleport", time.Now().Format(time.RFC3339), filepath.Base(filename)}
	key := strings.Join(keyElements, "/")

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return "", err
	}

	svc := s3.New(sess)

	input := &s3.PutObjectInput{
		Body:   file,
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err = svc.PutObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			return "", aerr
		}
		return "", err
	}

	return fmt.Sprintf("s3://%s/%s", bucket, key), nil
}
