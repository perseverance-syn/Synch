package engine

import (
	"strings"
	"testing"
)

// TestCredentialNameTable proves E3: infrastructure credential names are
// recognized by exact name, prefix, contained private-key marker, and suffix,
// while ordinary environment names pass.
func TestCredentialNameTable(t *testing.T) {
	for _, test := range []struct {
		name       string
		credential bool
	}{
		// The reviewer's reported gaps must now be flagged.
		{"AWS_BEARER_TOKEN_BEDROCK", true},
		{"STRIPE_SECRET_KEY", true},
		{"DJANGO_SECRET_KEY", true},
		{"PGPASSWORD", true},
		{"APIKEY", true},
		{"API_KEY", true},
		{"DOCKER_AUTH_CONFIG", true},
		{"SNOWFLAKE_PRIVATE_KEY_PATH", true},
		// Previously covered names stay covered, in any case.
		{"AWS_SECRET_ACCESS_KEY", true},
		{"aws_session_token", true},
		{"GITHUB_TOKEN", true},
		{"OPENAI_API_KEY", true},
		{"vault_token_file", true},
		{"service_password", true},
		{"deploy_secret", true},
		{"id_rsa_private_key_backup", true},
		{"AWS_BEARER_TOKEN_LAMBDA", true},
		// Ordinary environment names must pass.
		{"PATH", false},
		{"HOME", false},
		{"GOPATH", false},
		{"GOFLAGS", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := CredentialName(test.name); got != test.credential {
				t.Fatalf("CredentialName(%q) = %v, want %v", test.name, got, test.credential)
			}
		})
	}
	// ValidateEnv refuses every flagged name and reports it, without leaking values.
	if err := ValidateEnv(map[string]string{"PATH": "/bin", "API_KEY": "value", "PGPASSWORD": "value"}); err == nil ||
		!strings.Contains(err.Error(), "API_KEY") || strings.Contains(err.Error(), "value") {
		t.Fatalf("ValidateEnv credential check = %v", err)
	}
	if err := ValidateEnv(map[string]string{"PATH": "/bin", "GOPATH": "/go", "GOFLAGS": ""}); err != nil {
		t.Fatalf("ValidateEnv refused ordinary names: %v", err)
	}
}
