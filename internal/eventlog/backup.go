package eventlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// --- Snapshot publication: take an online backup or restore an offline log ---

// Backup publishes a consistent SQLite snapshot at a new destination.
// The destination filesystem must support hard links, which prevent overwrite.
func (s *Store) Backup(ctx context.Context, destPath string) error {
	temp, err := snapshot(ctx, s.readers, destPath)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp) }()
	if err = syncSnapshot(ctx, temp); err != nil {
		return err
	}
	if err = os.Link(temp, destPath); err != nil {
		return fmt.Errorf("publish backup: %w", err)
	}
	return syncDirectory(destPath)
}

// Restore replaces a closed data file. Both paths must be offline; if a
// sidecar remains, open and cleanly close that store before restoring.
func Restore(ctx context.Context, srcPath, dataPath string) (err error) {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	src, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("stat backup: %w", err)
	}
	dest, err := os.Stat(dataPath)
	if err == nil && os.SameFile(src, dest) {
		return errors.New("restore paths refer to the same file")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		_, err := os.Lstat(dataPath + suffix)
		if err == nil {
			return fmt.Errorf("restore destination has SQLite sidecar %s", suffix)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	db, err := connect(srcPath, "ro")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	temp, err := snapshot(ctx, db, dataPath)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp) }()
	if err := rotateEpoch(ctx, temp); err != nil {
		return fmt.Errorf("prepare restored log: %w", err)
	}
	if err = syncSnapshot(ctx, temp); err != nil {
		return err
	}
	if err := os.Rename(temp, dataPath); err != nil {
		return fmt.Errorf("publish restored log: %w", err)
	}
	return syncDirectory(dataPath)
}

// --- Snapshot preparation: copy, validate, and flush the replacement database ---

// snapshot copies a consistent SQLite view to a temporary file beside its destination.
func snapshot(ctx context.Context, db *sql.DB, dest string) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(dest), ".eventlog-snapshot-*")
	if err != nil {
		return "", fmt.Errorf("create snapshot file: %w", err)
	}
	path := f.Name()
	if err = f.Close(); err == nil {
		_, err = db.ExecContext(ctx, "VACUUM INTO ?", path)
	}
	if err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("snapshot event log: %w", err)
	}
	return path, nil
}

// rotateEpoch validates the backup format and gives the restored log a new replay epoch.
func rotateEpoch(ctx context.Context, path string) (err error) {
	db, err := connect(path, "rw")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	var current int
	if err = db.QueryRowContext(ctx, "SELECT max(version) FROM schema_version").Scan(&current); err != nil {
		return fmt.Errorf("read backup schema version: %w", err)
	}
	if current != version {
		return fmt.Errorf("unsupported backup schema version %d", current)
	}
	res, err := db.ExecContext(ctx, "UPDATE metadata SET epoch=?", newEpoch())
	if err != nil {
		return fmt.Errorf("update restored epoch: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("check restored epoch count: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("backup has %d epoch records", n)
	}
	return nil
}

// syncSnapshot flushes the snapshot before publication and checks for cancellation.
func syncSnapshot(ctx context.Context, path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open snapshot for sync: %w", err)
	}
	if err = errors.Join(f.Sync(), f.Close()); err != nil {
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if err := syncDirectory(path); err != nil {
		return err
	}
	return ctx.Err()
}

// syncDirectory flushes the directory entry where os.File supports directory sync.
func syncDirectory(path string) error {
	// Windows does not expose a portable directory fsync through os.File.
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(filepath.Dir(path))
	if err == nil {
		err = errors.Join(dir.Sync(), dir.Close())
	}
	if err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	return nil
}
