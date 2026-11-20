package policy

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// catalogueDateTime follows the full date-time checker in ajv-formats 3.0.1,
// pinned by contracts/check.mjs. The separator class mirrors ECMAScript \s.
var catalogueDateTime = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[Tt\t-\r \x{00a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}](\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)(?:[Zz]|([+-])(\d{2})(?::?(\d{2}))?)$`)

// parseCatalogueTime validates the wire spelling before converting it to a
// Go instant. time.Parse's RFC3339 grammar and ranges differ from the checker.
func parseCatalogueTime(s string) (time.Time, bool) {
	m := catalogueDateTime.FindStringSubmatch(s)
	if m == nil {
		return time.Time{}, false
	}
	date, err := time.Parse(time.DateOnly, m[1])
	if err != nil {
		return time.Time{}, false
	}
	hour, _ := strconv.Atoi(m[2])
	minute, _ := strconv.Atoi(m[3])
	seconds, _ := strconv.ParseFloat(m[4], 64) // same number comparison as the JS checker
	offsetHour, _ := strconv.Atoi(m[6])
	offsetMinute, _ := strconv.Atoi(m[7])
	if offsetHour > 23 || offsetMinute > 59 {
		return time.Time{}, false
	}
	if m[5] == "-" {
		offsetHour, offsetMinute = -offsetHour, -offsetMinute
	}
	if hour > 23 || minute > 59 || seconds >= 60 {
		// Match the checker's leap-second branch, including UTC rollover.
		utcHour, utcMinute := hour-offsetHour, minute-offsetMinute
		if utcMinute < 0 {
			utcHour--
		}
		if (utcHour != 23 && utcHour != -1) || (utcMinute != 59 && utcMinute != -1) || seconds >= 61 {
			return time.Time{}, false
		}
	}
	second, _ := strconv.Atoi(m[4][:2])
	var nanos int
	if len(m[4]) > 2 {
		fraction := m[4][3:] + "000000000"
		nanos, _ = strconv.Atoi(fraction[:9])
	}
	// Go's clock has no separate leap second. Place a literal :60 at the
	// final representable instant before the next minute.
	if second == 60 {
		second, nanos = 59, int(time.Second)-1
	}
	offset := (offsetHour*60 + offsetMinute) * 60
	zone := time.UTC
	if offset != 0 {
		zone = time.FixedZone("", offset)
	}
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, second, nanos, zone), true
}

// catalogueTime decodes a replay timestamp using the catalogue's format.
type catalogueTime struct{ time.Time }

// UnmarshalJSON validates a timestamp token and stores its Go instant.
func (t *catalogueTime) UnmarshalJSON(raw []byte) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	parsed, ok := parseCatalogueTime(s)
	if !ok {
		return fmt.Errorf("invalid date-time %q", s)
	}
	t.Time = parsed
	return nil
}
