package hyperv

import (
	"context"
	"encoding/json"
	"maps"

	"synch/internal/vmdriver"
)

// Host defines an optional remoting destination and operator-configured exports.
// Each base maps to a host-local exported .vmcx configuration path.
type Host struct {
	ComputerName string
	Exports      map[string]string
}

// Command contains only public PowerShell invocation data.
type Command struct {
	ComputerName string
	Script       string
	Arguments    []string
}

// Runner binds Arguments positionally as data to the script's param block.
// It runs locally when ComputerName is empty. For remoting it builds a
// PSCredential from the separate auth argument and passes it to Invoke-Command.
// Auth must stay out of scripts, process arguments, diagnostics and recordings.
// A runner invokes the command at most once and returns only its JSON output.
type Runner func(context.Context, Command, vmdriver.Credentials) ([]byte, error)

// RequestLoader is the journal read used to obtain the persisted snapshot owner.
type RequestLoader interface {
	Load(context.Context, string) (vmdriver.RequestRecord, error)
}

// Driver adapts Hyper-V cmdlets to the synchronous VM driver contract.
type Driver struct {
	hosts    map[string]Host
	runner   Runner
	broker   vmdriver.Broker
	requests RequestLoader
}

var _ vmdriver.VMDriver = (*Driver)(nil)

// New copies host configuration and wires the runner, broker and request reader.
func New(hosts map[string]Host, runner Runner, broker vmdriver.Broker, requests RequestLoader) (*Driver, error) {
	if len(hosts) == 0 || runner == nil {
		return nil, vmdriver.ErrInvalid
	}
	registered := make(map[string]Host, len(hosts))
	for endpoint, host := range hosts {
		if endpoint == "" || (host.ComputerName != "" && broker == nil) {
			return nil, vmdriver.ErrInvalid
		}
		host.Exports = maps.Clone(host.Exports)
		registered[endpoint] = host
	}
	return &Driver{registered, runner, broker, requests}, nil
}

// execute sanitizes transport failures and treats unreadable mutation results as uncertain.
func (d *Driver) execute(ctx context.Context, endpoint, script string, result any, mutating bool, args ...string) error {
	host, ok := d.hosts[endpoint]
	if !ok {
		return vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	var auth vmdriver.Credentials
	if host.ComputerName != "" {
		credentials, err := d.broker.Credentials(ctx, endpoint)
		if err != nil || credentials.Username == "" || credentials.Password == "" {
			return vmdriver.PreDispatch(vmdriver.ErrInvalid)
		}
		auth.Username, auth.Password = credentials.Username, credentials.Password
	}
	raw, err := d.runner(ctx, Command{host.ComputerName, script, args}, auth)
	if err != nil || json.Unmarshal(raw, result) != nil {
		if mutating {
			return vmdriver.ErrUncertain
		}
		return vmdriver.ErrProvider
	}
	return nil
}

// mutate decodes the explicit synchronous success or pre-mutation rejection.
func (d *Driver) mutate(ctx context.Context, endpoint, script string, args ...string) (vmdriver.TaskRef, error) {
	var complete *bool
	if err := d.execute(ctx, endpoint, script, &complete, true, args...); err != nil {
		return vmdriver.TaskRef{}, err
	}
	if complete == nil {
		return vmdriver.TaskRef{}, vmdriver.ErrUncertain
	}
	if !*complete {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	return vmdriver.TaskRef{}, nil
}
