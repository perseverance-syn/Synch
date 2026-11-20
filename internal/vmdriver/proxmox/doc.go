// Package proxmox implements the Proxmox VE QEMU transport for VM driver
// contract §§1, 3 and 4 using the API2 JSON endpoints. It owns API-token
// authentication and evidence collection. The shared dispatcher owns policy,
// ownership, journal admission and the real-host advertisement gate.
package proxmox
