// Package routing selects a qualified harness/model pair and explains the decision.
// It implements decisions 03 and 11, engine-adapter §2, and the routing inputs
// to thread-state-machine T05, T06, and T21. It honors operator selections
// after harness failure under decision 07. The daemon owns policy, evidence
// storage, event persistence, and thread transitions.
package routing
