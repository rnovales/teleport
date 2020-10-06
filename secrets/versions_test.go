package secrets_test

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/rnovales/teleport/secrets"
	"github.com/stretchr/testify/assert"
)

func TestVersion1Decryption(t *testing.T) {
	var (
		secretKey = "SAMPLE90odbSuT8aS12nUFjPlUg1ip0AAeQg0wiJKzv318auuDh0C6zKDmTrfqWwqGvA0O"
		key       = "USER_TOKEN"
		value     = "SAMPLELq6FdAp3Rjv"
	)

	settings := secrets.Settings{"TESTAPP", "TESTAPP_SECRET_KEY", "testdata/secrets.txt.version1.enc"}

	os.Setenv(settings.KeyEnvVariable, secretKey)
	defer os.Unsetenv(settings.KeyEnvVariable)

	body, err := secrets.ReadSecretsFile(settings)
	assert.NoError(t, err)

	assert.Equal(t, body[0].Key, key)
	assert.Equal(t, body[0].Value, value)
}

func TestVersion1ConversionToVersion2(t *testing.T) {
	var (
		secretKey = "SAMPLE90odbSuT8aS12nUFjPlUg1ip0AAeQg0wiJKzv318auuDh0C6zKDmTrfqWwqGvA0O"
		key       = "USER_TOKEN"
		value     = "SAMPLELq6FdAp3Rjv"
		key2      = "API_TOKEN"
		value2    = "SAMPLEHgkGJ4JYBTo"
	)

	os.Setenv(settings.KeyEnvVariable, secretKey)
	defer os.Unsetenv(settings.KeyEnvVariable)

	tmpFile, err := ioutil.TempFile(os.TempDir(), "secrets")
	if err != nil {
		t.Fatal("cannot create temporary file:", err)
	}
	defer os.Remove(tmpFile.Name())

	source, err := os.Open("testdata/secrets.txt.version1.enc")
	assert.NoError(t, err)
	defer source.Close()
	_, err = io.Copy(tmpFile, source)
	assert.NoError(t, err)

	settings := secrets.Settings{"TESTAPP", "TESTAPP_SECRET_KEY", tmpFile.Name()}

	version, err := secrets.EncryptionVersion(settings)
	assert.NoError(t, err)
	assert.Equal(t, version, 1)

	err = secrets.UpdateSecret(settings, key2, value2)
	assert.NoError(t, err)

	version, err = secrets.EncryptionVersion(settings)
	assert.NoError(t, err)
	assert.Equal(t, version, 2)

	body, err := secrets.ReadSecretsFile(settings)
	assert.NoError(t, err)
	assert.Equal(t, body[0].Key, key2)
	assert.Equal(t, body[0].Value, value2)
	assert.Equal(t, body[1].Key, key)
	assert.Equal(t, body[1].Value, value)
}

func TestVersion2Decryption(t *testing.T) {
	var (
		secretKey = "SAMPLE90odbSuT8aS12nUFjPlUg1ip0AAeQg0wiJKzv318auuDh0C6zKDmTrfqWwqGvA0O"
		key       = "USER_TOKEN"
		value     = "SAMPLELq6FdAp3Rjv"
	)

	settings := secrets.Settings{"TESTAPP", "TESTAPP_SECRET_KEY", "testdata/secrets.txt.version2.enc"}

	os.Setenv(settings.KeyEnvVariable, secretKey)
	defer os.Unsetenv(settings.KeyEnvVariable)

	body, err := secrets.ReadSecretsFile(settings)
	assert.NoError(t, err)

	assert.Equal(t, body[0].Key, key)
	assert.Equal(t, body[0].Value, value)
}
