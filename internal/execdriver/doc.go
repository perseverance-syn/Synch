// Package execdriver defines the execution-driver contract and admits Host access
// processes under the daemon's host account. It implements execution-driver
// sections 1, 2, 4 and the settlement gate in section 5. The supervisor owns
// process trees and emits lifecycle facts. Policy, credentials, event storage
// and mode change orchestration belong to callers.
// D-17 protocol processes expose caller-owned stdin and stdout through Proc.
package execdriver
