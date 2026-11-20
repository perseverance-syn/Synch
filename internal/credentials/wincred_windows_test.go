package credentials

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"
)

// TestWindowsCredentialManager proves C1 on the real Credential Manager: a throwaway
// thread-scoped secret round-trips through CredWriteW/CredReadW and CredDeleteW removes it.
func TestWindowsCredentialManager(t *testing.T) {
	kc := windowsKeychain()
	if kc == nil {
		t.Fatal("Credential Manager unavailable on Windows")
	}
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	scope := ThreadScope("test-" + hex.EncodeToString(id[:]))
	s := NewStore(kc)
	t.Cleanup(func() { _ = s.Delete(scope, "throwaway") })
	if _, err := s.get(scope, "throwaway"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("before write: %v", err)
	}
	value := "throwaway-" + hex.EncodeToString(id[:])
	if err := s.Put(scope, "throwaway", value); err != nil {
		t.Fatal(err)
	}
	if got, err := s.get(scope, "throwaway"); err != nil || got != value {
		t.Fatalf("read %q, %v", got, err)
	}
	acct, err := account(scope, "throwaway")
	if err != nil {
		t.Fatal(err)
	}
	if err := kc.Delete(acct); err != nil {
		t.Fatal(err)
	}
	if _, err := s.get(scope, "throwaway"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if err := kc.Delete(acct); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}
