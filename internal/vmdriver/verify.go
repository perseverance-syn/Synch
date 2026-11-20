package vmdriver

import "context"

// SnapshotVerifier is implemented by drivers that can independently verify a
// snapshot on a VM without changing it.
type SnapshotVerifier interface {
	VerifySnapshot(ctx context.Context, endpoint, vm, name string) error
}

// VerifySnapshot checks that name is a unique, intact snapshot of vm through its
// owning driver. It is read-only. A driver that cannot verify refuses with ErrSnapshot.
func VerifySnapshot(ctx context.Context, d VMDriver, endpoint, vm, name string) error {
	v, ok := d.(SnapshotVerifier)
	if !ok || endpoint == "" || vm == "" || name == "" {
		return ErrSnapshot
	}
	return v.VerifySnapshot(ctx, endpoint, vm, name)
}
