// Package workstation implements the VMware Workstation transport for VM driver
// contract §§1, 3 and 4. Lifecycle and snapshot operations run the `vmrun` CLI
// through an injected Runner; inventory, registration and the ownership marker
// (the .vmx `annotation` parameter) use the `vmrest` REST API with basic auth
// from an injected vmdriver.Broker. It never runs a real binary on its own and
// never contacts a hypervisor in tests. The shared dispatcher owns policy,
// ownership, journal admission, serialization per (host, VM) and the real-host
// advertisement gate. Workstation has no task API, so get_task is unavailable,
// and snapshots carry no token (Q-55): a lost snapshot response stays unknown.
package workstation
