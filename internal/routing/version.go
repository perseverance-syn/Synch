package routing

import (
	"slices"
	"strconv"
	"strings"
)

// versionInRange checks all whitespace-separated numeric version bounds in a declaration.
// A bare version is an exact match. Unrecognized syntax leaves the pair unqualified.
func versionInRange(installed, declaration string) bool {
	version, ok := parseVersion(installed)
	bounds := strings.Fields(declaration)
	if !ok || len(bounds) == 0 {
		return false
	}
	for _, bound := range bounds {
		operator := strings.TrimRight(bound, "0123456789.")
		target, ok := parseVersion(strings.TrimPrefix(bound, operator))
		if !ok || !matchesBound(slices.Compare(version[:], target[:]), operator) {
			return false
		}
	}
	return true
}

// parseVersion accepts stable major.minor[.patch] releases and treats an omitted patch as zero.
func parseVersion(raw string) ([3]uint64, bool) {
	var version [3]uint64
	parts := strings.Split(raw, ".")
	if len(parts) < 2 || len(parts) > len(version) {
		return version, false
	}
	for i, part := range parts {
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil || len(part) > 1 && part[0] == '0' {
			return version, false
		}
		version[i] = n
	}
	return version, true
}

// matchesBound interprets the ordering of a release relative to a single bound.
func matchesBound(order int, operator string) bool {
	switch operator {
	case "", "=":
		return order == 0
	case ">=":
		return order >= 0
	case ">":
		return order > 0
	case "<=":
		return order <= 0
	case "<":
		return order < 0
	default:
		return false
	}
}
