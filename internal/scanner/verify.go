package scanner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// verifiedCopy hashes bytes while staging them to prevent a mutable source
// path from swapping the verified program before execution.
func (d *Driver) verifiedCopy(ctx context.Context, dir string) (string, BinaryInfo, error) {
	info := d.pin.BinaryInfo
	digest, err := hex.DecodeString(info.SHA256)
	if err != nil || len(digest) != sha256.Size || info.Version == "" || d.pin.Path == "" {
		return "", BinaryInfo{}, ErrChecksumMismatch
	}
	info.SHA256 = strings.ToLower(info.SHA256)
	name := d.tool
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(dir, name)
	actual, err := copyBinary(d.pin.Path, path)
	if err != nil {
		return "", BinaryInfo{}, fmt.Errorf("stage scanner binary: %w", errors.Join(ErrChecksumMismatch, ErrBinaryUnreadable))
	}
	if actual != info.SHA256 {
		return "", BinaryInfo{}, ErrChecksumMismatch
	}
	if d.pin.SignaturePublished && (d.pin.VerifySignature == nil || d.pin.VerifySignature(ctx, path) != nil) {
		return "", BinaryInfo{}, ErrSignature
	}
	if err := ctx.Err(); err != nil {
		return "", BinaryInfo{}, err
	}
	if err := os.Chmod(path, 0500); err != nil {
		return "", BinaryInfo{}, errors.Join(ErrChecksumMismatch, ErrBinaryUnreadable)
	}
	return path, info, nil
}

// hashFile returns the lowercase hex SHA-256 of a file.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, f)
	if err := errors.Join(copyErr, f.Close()); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// copyBinary stages a regular file without making it executable before verification.
func copyBinary(source, destination string) (string, error) {
	stat, err := os.Stat(source)
	if err != nil {
		return "", err
	}
	if !stat.Mode().IsRegular() {
		return "", ErrBinaryUnreadable
	}
	input, err := os.Open(source)
	if err != nil {
		return "", err
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", errors.Join(err, input.Close())
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(output, hash), input)
	if err := errors.Join(copyErr, output.Close(), input.Close()); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
