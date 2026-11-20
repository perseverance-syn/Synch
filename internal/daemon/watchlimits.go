package daemon

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Built-in child watchdog limits, used when neither the daemon nor the project sets one.
const (
	defaultMaxRuntime  = 2 * time.Hour
	defaultLoopRepeats = 5
)

// Setting is a watchdog value written as text or, for counts, a JSON number:
// "" leaves it unset, "off" (or 0) disables the check, anything else is a value.
type Setting string

// UnmarshalJSON accepts a JSON string or number.
func (s *Setting) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		var number json.Number
		if err := json.Unmarshal(raw, &number); err != nil {
			return fmt.Errorf("a watchdog setting must be a string or number: %s", raw)
		}
		text = number.String()
	}
	*s = Setting(text)
	return nil
}

// watchLimits is the effective watchdog configuration of one child; zero disables a check.
type watchLimits struct {
	runtime time.Duration
	repeats int
}

// limitOverride is a parsed setting layer; nil means that layer leaves the value unset.
type limitOverride struct {
	runtime *time.Duration
	repeats *int
}

// parseOverride validates one layer of settings; name says where it came from.
func parseOverride(name string, runtime, repeats Setting) (limitOverride, error) {
	var o limitOverride
	if v := strings.TrimSpace(string(runtime)); v != "" {
		d := time.Duration(0)
		if v != "off" && v != "0" {
			var err error
			if d, err = time.ParseDuration(v); err != nil || d <= 0 {
				return o, fmt.Errorf("%s max runtime %q must be a positive Go duration such as 2h, or off", name, v)
			}
		}
		o.runtime = &d
	}
	if v := strings.TrimSpace(string(repeats)); v != "" {
		n := 0
		if v != "off" && v != "0" {
			var err error
			if n, err = strconv.Atoi(v); err != nil || n < 2 {
				return o, fmt.Errorf("%s loop repeats %q must be an integer of at least 2, or off", name, v)
			}
		}
		o.repeats = &n
	}
	return o, nil
}

// apply layers an override over base.
func (o limitOverride) apply(base watchLimits) watchLimits {
	if o.runtime != nil {
		base.runtime = *o.runtime
	}
	if o.repeats != nil {
		base.repeats = *o.repeats
	}
	return base
}

// resolveWatch validates the daemon default and every project override and
// returns the default (override over the built-in values) and each project's effective limits.
func resolveWatch(cfg Config) (watchLimits, map[string]watchLimits, error) {
	layer, err := parseOverride("daemon default (SYNCH_CHILD_MAX_RUNTIME, SYNCH_CHILD_LOOP_REPEATS):", cfg.ChildMaxRuntime, cfg.ChildLoopRepeats)
	if err != nil {
		return watchLimits{}, nil, err
	}
	def := layer.apply(watchLimits{runtime: defaultMaxRuntime, repeats: defaultLoopRepeats})
	projects := make(map[string]watchLimits, len(cfg.Projects))
	for id, p := range cfg.Projects {
		o, err := parseOverride("project "+id+":", p.MaxRuntime, p.LoopRepeats)
		if err != nil {
			return watchLimits{}, nil, err
		}
		projects[id] = o.apply(def)
	}
	return def, projects, nil
}

// watchFor returns the effective watchdog limits for a project.
func (d *Daemon) watchFor(project string) watchLimits {
	if l, ok := d.projectWatch[project]; ok {
		return l
	}
	return d.defaultWatch
}
