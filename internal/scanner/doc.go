// Package scanner verifies and runs pinned scanner programs and retains redacted
// reports. It implements scanner-driver sections 1–5 and Q-37. It does not
// resolve source revisions, download tools or databases, or wire the gate.
//
// The staged binary is hashed while copied and re-hashed immediately before
// exec. Scratch must be service-account-only: on Windows, mode bits do not block
// same-user writes, so the remaining window is closed by the directory ACL.
package scanner
