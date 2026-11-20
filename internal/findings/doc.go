// Package findings owns normalized scanner findings, fingerprint groups and fix
// attempt records. It implements scanner-driver section 4 and the Findings and
// fixes workflow. It does not run tools, open changes itself or wire the gate.
package findings
