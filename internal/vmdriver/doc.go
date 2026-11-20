// Package vmdriver owns the V1 provider interface, authorization and durable
// dispatch/reconciliation rules in VM driver contract v3.1.0 §§1–5. The daemon
// supplies durable storage, policy decisions and thread state. Provider packages
// own the transport and never expose infrastructure credentials to workers.
package vmdriver
