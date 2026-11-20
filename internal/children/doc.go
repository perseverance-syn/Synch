// Package children coordinates managed child work under thread-state-machine
// §2.1 G.5 and §2.2 T08, T09, T27, T30, and T31, with limits from Q-27.
// It owns scheduling inputs and assignment, result, and audit-setting records
// under decisions 06 and 08. The thread machine owns transitions, the
// supervisor owns process trees, and the caller owns routing and workspaces.
package children
