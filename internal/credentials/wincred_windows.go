package credentials

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

// --- Windows Credential Manager through advapi32 (stdlib syscall, no new dependency) ---

var (
	advapi32        = syscall.NewLazyDLL("advapi32.dll")
	procCredWriteW  = advapi32.NewProc("CredWriteW")
	procCredReadW   = advapi32.NewProc("CredReadW")
	procCredDeleteW = advapi32.NewProc("CredDeleteW")
	procCredFree    = advapi32.NewProc("CredFree")
)

const (
	credTypeGeneric         = 1
	credPersistLocalMachine = 2
	// credMaxBlob is CRED_MAX_CREDENTIAL_BLOB_SIZE (5 * 512 bytes).
	credMaxBlob = 5 * 512
	// errorNotFound is ERROR_NOT_FOUND, returned when no credential has the target name.
	errorNotFound = syscall.Errno(1168)
)

// credential mirrors the Win32 CREDENTIALW structure.
type credential struct {
	Flags              uint32
	Type               uint32
	TargetName         *uint16
	Comment            *uint16
	LastWritten        syscall.Filetime
	CredentialBlobSize uint32
	CredentialBlob     *byte
	Persist            uint32
	AttributeCount     uint32
	Attributes         uintptr
	TargetAlias        *uint16
	UserName           *uint16
}

// wincred stores generic credentials named "synch:<account>" for the current user.
type wincred struct{}

// windowsKeychain returns Credential Manager when advapi32 exposes the credential API.
func windowsKeychain() Keychain {
	if advapi32.Load() != nil || procCredWriteW.Find() != nil {
		return nil
	}
	return wincred{}
}

// Kind names the backend.
func (wincred) Kind() string { return "wincred" }

// target is the Credential Manager target name of an account.
func target(acct string) (*uint16, error) {
	return syscall.UTF16PtrFromString(Service + ":" + acct)
}

// Set writes the secret as the credential blob, replacing any previous value.
func (wincred) Set(acct, secret string) error {
	if secret == "" || len(secret) > credMaxBlob {
		return fmt.Errorf("%w: secret size outside the Credential Manager limits", ErrInvalid)
	}
	name, err := target(acct)
	if err != nil {
		return fmt.Errorf("%w: target name", ErrInvalid)
	}
	user, _ := syscall.UTF16PtrFromString(Service)
	blob := []byte(secret)
	c := credential{Type: credTypeGeneric, TargetName: name, Persist: credPersistLocalMachine,
		CredentialBlobSize: uint32(len(blob)), CredentialBlob: &blob[0], UserName: user}
	if ok, _, callErr := procCredWriteW.Call(uintptr(unsafe.Pointer(&c)), 0); ok == 0 {
		return fmt.Errorf("credentials: CredWriteW: %w", callErr)
	}
	return nil
}

// Get copies the blob out and frees the system buffer.
func (wincred) Get(acct string) (string, error) {
	name, err := target(acct)
	if err != nil {
		return "", fmt.Errorf("%w: target name", ErrInvalid)
	}
	// CredReadW fills pcred with a system-allocated CREDENTIALW that CredFree releases.
	var pcred *credential
	ok, _, callErr := procCredReadW.Call(uintptr(unsafe.Pointer(name)), credTypeGeneric, 0, uintptr(unsafe.Pointer(&pcred)))
	if ok == 0 {
		if errors.Is(callErr, errorNotFound) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("credentials: CredReadW: %w", callErr)
	}
	defer func() { _, _, _ = procCredFree.Call(uintptr(unsafe.Pointer(pcred))) }()
	if pcred.CredentialBlobSize == 0 || pcred.CredentialBlob == nil {
		return "", ErrNotFound
	}
	return string(unsafe.Slice(pcred.CredentialBlob, pcred.CredentialBlobSize)), nil
}

// Delete removes the credential; an absent one is ErrNotFound.
func (wincred) Delete(acct string) error {
	name, err := target(acct)
	if err != nil {
		return fmt.Errorf("%w: target name", ErrInvalid)
	}
	if ok, _, callErr := procCredDeleteW.Call(uintptr(unsafe.Pointer(name)), credTypeGeneric, 0); ok == 0 {
		if errors.Is(callErr, errorNotFound) {
			return ErrNotFound
		}
		return fmt.Errorf("credentials: CredDeleteW: %w", callErr)
	}
	return nil
}
