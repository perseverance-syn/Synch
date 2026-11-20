// Package esxi implements VM driver contract §§1, 3 and 4 using vSphere SOAP.
// It owns transport, sessions and evidence. vCenter supplies cloning for
// managed ESXi hosts. The shared dispatcher owns policy and the journal.
package esxi
