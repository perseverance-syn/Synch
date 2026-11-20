// Package virtualbox implements the Oracle VirtualBox transport for VM driver
// contract §§1, 3 and 4 by running `VBoxManage` through an injected Runner. The
// ownership marker (`synch/token`, `synch/owner` extradata) and snapshot
// descriptions carry the request token. It never runs a real binary on its own
// and never contacts a hypervisor in tests. VBoxManage is a local tool and takes
// no credentials. The shared dispatcher owns policy, ownership, journal
// admission, serialization per (host, VM) and the real-host advertisement gate.
// VirtualBox has no task API, so get_task is unavailable (Q-40).
package virtualbox
