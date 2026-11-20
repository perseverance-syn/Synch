package vmdriver

import "time"

type Basis string
type Disposition string

const (
	Created   Basis       = "created"
	Assigned  Basis       = "assigned"
	Keep      Disposition = "keep"
	Discard   Disposition = "discard"
	Undecided Disposition = "undecided"
)

// Ownership records agent creation or a trusted assignment. The daemon writes
// Assigned records only through its authenticated operator API.
type Ownership struct {
	Endpoint    string
	VMRef       string
	OwnerThread string
	Basis       Basis
	AssignedBy  string
	AssignedAt  time.Time
	Disposition Disposition
}

// CanMutate checks exact resource and thread ownership, including assignment provenance.
func (o Ownership) CanMutate(endpoint, vm, thread string) bool {
	return endpoint != "" && vm != "" && thread != "" &&
		o.Endpoint == endpoint && o.VMRef == vm && o.OwnerThread == thread &&
		(o.Basis == Created || (o.Basis == Assigned && o.AssignedBy != "" && !o.AssignedAt.IsZero()))
}

// Orphans returns visible VMs whose owner is terminal and has made no disposition.
func Orphans(endpoint string, inventory []VM, ownership []Ownership, terminal func(string) bool) []Ownership {
	visible := make(map[string]bool, len(inventory))
	for _, vm := range inventory {
		visible[vm.Ref] = true
	}
	var orphans []Ownership
	for _, o := range ownership {
		if o.Endpoint == endpoint && visible[o.VMRef] && o.OwnerThread != "" &&
			o.Disposition == Undecided && terminal(o.OwnerThread) {
			orphans = append(orphans, o)
		}
	}
	return orphans
}
