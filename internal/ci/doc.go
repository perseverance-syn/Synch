// Package ci owns the provider-neutral half of the CI integrations: the
// Provider interface, the mapping of provider verdicts to the five gate check
// statuses, commit-SHA binding of results, the decision 18 trigger dispatcher
// (persist before dispatch, reconcile by token, never blindly repeat), log
// redaction, capping and retention by reference, and the shared HTTP helper.
// Subpackages github and gitlab implement Provider for GitHub Actions and
// GitLab CI. It does not own the gate file or its parsers (internal/gates:
// CI runs the same definition, one job per check), event-log storage (the
// daemon supplies Journal and LogStore), or the credentials broker (card 401;
// TokenBroker here is a minimal interpretation to be replaced). Jenkins is
// card 504.
//
// It implements contracts/drivers/ci-driver.md (proposed v0.1.0) sections 1-8.
package ci
