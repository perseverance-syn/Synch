package hyperv

import (
	"context"
	"errors"
	"strings"
	"testing"

	"synch/internal/vmdriver"
)

const vmID = "12345678-1234-1234-1234-123456789abc"

// TestInventory verifies the Get-VM JSON projection and synchronous task contract (W1).
func TestInventory(t *testing.T) {
	ctx := context.Background()
	d, err := New(map[string]Host{"lab": {}}, func(_ context.Context, command Command, _ vmdriver.Credentials) ([]byte, error) {
		if !strings.Contains(command.Script, "Get-VM") || !strings.Contains(command.Script, "ConvertTo-Json") {
			t.Fatalf("unexpected command: %s", command.Script)
		}
		return []byte(`[{"Id":"` + vmID + `","Name":"Lab","State":"Off","Notes":""}]`), nil
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	vms, err := d.Inventory(ctx, "lab")
	if err != nil || len(vms) != 1 || vms[0] != (vmdriver.VM{Ref: vmID, Name: "Lab", State: "Off"}) {
		t.Fatalf("inventory = %+v, %v", vms, err)
	}
	if _, err := d.GetTask(ctx, "lab", vmdriver.TaskRef{}); !errors.Is(err, vmdriver.ErrUnavailable) {
		t.Fatalf("GetTask = %v", err)
	}
}
