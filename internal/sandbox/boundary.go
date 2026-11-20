package sandbox

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"synch/internal/eventlog"
	"synch/internal/vmdriver"
)

// --- Boundary checks: a configuration is advertised only after all pass ---

// Evidence results.
const (
	Passed = "passed"
	Failed = "failed"
	Error  = "error"
	NotRun = "not run"
)

// Check names. RealHostCertification is separate evidence and never gates advertising on fakes.
const (
	FilesystemCheck       = "filesystem"
	CredentialsCheck      = "credentials"
	NetworkCheck          = "network"
	RealHostCertification = "real_host_certification"
)

// Evidence is one check result for one configuration.
type Evidence struct {
	Configuration, Check, Result, Reason string
}

// Mount is a host path visible inside the VM.
type Mount struct {
	HostPath string
	Writable bool
}

// Properties are the boundary-relevant facts observed inside a VM.
type Properties struct {
	// Mounts lists host paths visible inside the VM.
	Mounts []Mount
	// Credentials lists credential material reachable inside (files, sockets, keychains).
	Credentials []string
	// Env is the default environment a process inside the VM receives.
	Env []string
	// Egress lists network destinations reachable from inside.
	Egress []string
}

// Inspector observes a VM created from a configuration. A real-host inspector probes
// the guest; the test fakes return their declared properties.
type Inspector interface {
	Inspect(ctx context.Context, cfg Configuration, vm vmdriver.VM) (Properties, error)
}

// Qualify runs the boundary checks on a fresh disposable VM per configuration,
// advertises exactly the configurations whose three checks all passed, and returns
// the evidence, including real-host certification as not run: deferred by owner.
func (d *Driver) Qualify(ctx context.Context) []Evidence {
	var all []Evidence
	var advertised []string
	for _, cfg := range d.cfg.Configurations {
		checks := d.qualify(ctx, cfg)
		if !slices.ContainsFunc(checks, func(e Evidence) bool { return e.Result != Passed }) {
			advertised = append(advertised, cfg.Name)
		}
		all = append(all, checks...)
		all = append(all, Evidence{cfg.Name, RealHostCertification, NotRun, "deferred by owner"})
	}
	d.mu.Lock()
	d.advertised, d.evidence = advertised, slices.Clone(all)
	d.mu.Unlock()
	return all
}

// Evidence returns the latest Qualify results.
func (d *Driver) Evidence() []Evidence {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.evidence)
}

// qualify creates, inspects and destroys one VM and evaluates the three boundary checks.
func (d *Driver) qualify(ctx context.Context, cfg Configuration) []Evidence {
	all := func(result, reason string) []Evidence {
		var out []Evidence
		for _, check := range []string{FilesystemCheck, CredentialsCheck, NetworkCheck} {
			out = append(out, Evidence{cfg.Name, check, result, reason})
		}
		return out
	}
	if d.cfg.Inspect == nil {
		return all(NotRun, "no boundary inspector configured")
	}
	// The qualification VM has its own owner identity, so it never counts as a task VM.
	owner := eventlog.NewID()
	vm, err := d.create(ctx, cfg, owner, NamePrefix+strings.ToLower(owner))
	if err != nil {
		return all(Error, err.Error())
	}
	props, inspectErr := d.cfg.Inspect.Inspect(ctx, cfg, vm)
	if err := d.destroy(context.WithoutCancel(ctx), &task{thread: owner, cfg: cfg, vm: vm}); err != nil {
		// A configuration whose disposable VM cannot be deleted is not disposable.
		return all(Error, err.Error())
	}
	if inspectErr != nil {
		return all(Error, inspectErr.Error())
	}
	return []Evidence{filesystem(cfg, props), d.credentials(cfg, props), network(cfg, props)}
}

// filesystem fails when any host path is mounted inside the VM, writable or not.
func filesystem(cfg Configuration, p Properties) Evidence {
	var found []string
	for _, m := range p.Mounts {
		mode := "read-only"
		if m.Writable {
			mode = "writable"
		}
		found = append(found, m.HostPath+" ("+mode+")")
	}
	if len(found) > 0 {
		return Evidence{cfg.Name, FilesystemCheck, Failed, "host paths mounted: " + strings.Join(found, ", ")}
	}
	return Evidence{cfg.Name, FilesystemCheck, Passed, "no host path mounted"}
}

// credentials fails when credential material is reachable or the default environment holds a known secret.
func (d *Driver) credentials(cfg Configuration, p Properties) Evidence {
	if len(p.Credentials) > 0 {
		return Evidence{cfg.Name, CredentialsCheck, Failed, "credential material reachable: " + strings.Join(p.Credentials, ", ")}
	}
	for _, secret := range d.cfg.KnownSecrets() {
		for _, e := range p.Env {
			if secret != "" && strings.Contains(e, secret) {
				name, _, _ := strings.Cut(e, "=")
				return Evidence{cfg.Name, CredentialsCheck, Failed, "known secret in VM environment variable " + name}
			}
		}
	}
	return Evidence{cfg.Name, CredentialsCheck, Passed, "no host or user credential reachable"}
}

// network fails when any reachable destination is outside the configuration's declared egress.
func network(cfg Configuration, p Properties) Evidence {
	var extra []string
	for _, dest := range p.Egress {
		if !slices.Contains(cfg.Egress, dest) {
			extra = append(extra, dest)
		}
	}
	if len(extra) > 0 {
		return Evidence{cfg.Name, NetworkCheck, Failed, "undeclared egress: " + strings.Join(extra, ", ")}
	}
	return Evidence{cfg.Name, NetworkCheck, Passed, fmt.Sprintf("egress limited to %d declared destinations", len(cfg.Egress))}
}
