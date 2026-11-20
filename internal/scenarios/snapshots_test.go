package scenarios

import (
	"testing"
)

// TestLostSnapshotResponseOnEveryDriver reuses each provider's lost-response fixture.
func TestLostSnapshotResponseOnEveryDriver(t *testing.T) {
	cases := []struct{ name, pkg, pattern, outcome string }{
		{"Proxmox", "./internal/vmdriver", "^TestProviderContract$/^Proxmox$/^lost_create_and_snapshot$", "reconciles"},
		{"ESXi", "./internal/vmdriver", "^TestProviderContract$/^ESXi$/^lost_create_and_snapshot$", "reconciles"},
		{"Workstation", "./internal/vmdriver/workstation", "^TestLostSnapshotStaysUnknown$", "pauses unknown"},
		{"VirtualBox", "./internal/vmdriver/virtualbox", "^TestLostSnapshotReconciles$", "reconciles"},
		{"Hyper-V", "./internal/vmdriver/hyperv", "^TestLostResponse$/^snapshot$", "reconciles"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runExistingFixture(t, tc.pkg, tc.pattern)
			t.Logf("%s; real host deferred by owner", tc.outcome)
		})
	}
}
