package secrets_test

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/rnovales/teleport/secrets"
	"github.com/stretchr/testify/assert"
)

var (
	settings = secrets.Settings{"TESTAPP", "TESTAPP_SECRET_KEY", ""}
)

func TestInitializeAndHeaderEncoding(t *testing.T) {
	withSecretsFile(t, func(settings secrets.Settings) {
		_, err := secrets.ReadSecretsFile(settings)
		assert.NoError(t, err)
	})
}

func TestUpdateSecret(t *testing.T) {
	withSecretsFile(t, func(settings secrets.Settings) {
		var (
			key    = "USER_TOKEN"
			value1 = "SAMPLE1joxLCJTYWx0Ijoie"
			value2 = "SAMPLE2bUx6S0wvMUg3aG0z"
		)

		// Create new key
		err := secrets.UpdateSecret(settings, key, value1)
		assert.NoError(t, err)

		contents, _ := ioutil.ReadFile(settings.SecretsFile)
		assert.Contains(t, string(contents), key, "keys are shown in plain text")
		assert.NotContains(t, string(contents), value1, "secrets are not shown in plain text")

		body, err := secrets.ReadSecretsFile(settings)
		assert.NoError(t, err)

		assert.Len(t, body, 1)
		assert.Equal(t, value1, body[0].Value)

		// Update key with new value
		err = secrets.UpdateSecret(settings, key, value2)
		assert.NoError(t, err)
		body, err = secrets.ReadSecretsFile(settings)
		assert.NoError(t, err)

		assert.Len(t, body, 1)
		assert.Equal(t, value2, body[0].Value)
	})
}

func TestGenerateSecretKey(t *testing.T) {
	secretKey, err := secrets.GenerateSecretKey()
	assert.NoError(t, err)
	assert.Len(t, secretKey, 64)
}

func TestDeleteSecret(t *testing.T) {
	withSecretsFile(t, func(settings secrets.Settings) {
		var (
			key    = "USER_TOKEN"
			value1 = "SAMPLE1joxLCJTYWx0Ijoie"
		)

		err := secrets.UpdateSecret(settings, key, value1)
		assert.NoError(t, err)

		err = secrets.DeleteSecret(settings, key)
		assert.NoError(t, err)

		contents, _ := ioutil.ReadFile(settings.SecretsFile)
		assert.NotContains(t, string(contents), key)
	})
}

func TestErrorOnWrongSecretKey(t *testing.T) {
	withSecretsFile(t, func(settings secrets.Settings) {
		var (
			key    = "USER_TOKEN"
			value1 = "SAMPLE1joxLCJTYWx0Ijoie"
		)
		err := secrets.UpdateSecret(settings, key, value1)
		assert.NoError(t, err)

		os.Setenv(settings.KeyEnvVariable, "DIFFERENT-KEYVAm87vcD")

		_, err = secrets.ReadSecretsFile(settings)
		if assert.Error(t, err) {
			assert.Equal(t, errors.New("cipher: message authentication failed (verify your secret key ENV variable)"), err)
		}
	})
}

func TestDoesNotReEncryptAllValuesOnUpdate(t *testing.T) {
	withSecretsFile(t, func(settings secrets.Settings) {
		err := secrets.UpdateSecret(settings, "ATOKEN", "SAMPLEFVePBeox1G")
		assert.NoError(t, err)
		err = secrets.UpdateSecret(settings, "BTOKEN", "SAMPLEFtp0Hx1j3q")
		assert.NoError(t, err)
		originalContents, err := ioutil.ReadFile(settings.SecretsFile)
		assert.NoError(t, err)

		err = secrets.UpdateSecret(settings, "CTOKEN", "SAMPLEinUglz69ZhE")
		assert.NoError(t, err)
		finalContents, err := ioutil.ReadFile(settings.SecretsFile)
		assert.NoError(t, err)

		// Since keys are sorted alphabetically and updating a new secret does not
		// modify the encryptedValues of the other secrets, the original file contents
		// (with ATOKEN and BTOKEN) should still be intact when we add a key that comes
		// later in the alphabet to the end of the file.
		assert.Contains(t, string(finalContents), string(originalContents))
	})

}

func withSecretsFile(t *testing.T, testfn func(secrets.Settings)) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "secrets")
	if err != nil {
		t.Fatal("cannot create temporary file:", err)
	}
	defer os.Remove(tmpFile.Name())

	os.Setenv(settings.KeyEnvVariable, "SAMPLExU1lBMkZtS2czMUR3")
	defer os.Unsetenv(settings.KeyEnvVariable)

	settings.SecretsFile = tmpFile.Name()
	secrets.InitializeSecretsFile(settings)

	testfn(settings)
}
