package daemon

import (
	"synch/internal/api"
	"synch/internal/execdriver"
)

func (d *Daemon) executionDrivers() []api.ExecutionDriver {
	result := []api.ExecutionDriver{}
	for _, kind := range []string{execdriver.KindDirect, execdriver.KindSandbox, execdriver.KindRemote} {
		driver := d.Driver(kind)
		if driver == nil {
			continue
		}
		info := driver.Describe()
		configs := append([]string{}, info.CertifiedConfigs...)
		status := "ready"
		if kind == execdriver.KindRemote {
			st := d.remote.Status()
			if !st.Connected {
				status = "disconnected"
			} else if st.Degraded {
				status = "degraded"
			}
		}
		result = append(result, api.ExecutionDriver{Kind: kind, Description: info.Label, Status: status, EvidenceIntegrity: info.EvidenceIntegrity, CertifiedConfigs: configs})
	}
	return result
}
