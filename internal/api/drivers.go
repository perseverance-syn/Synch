package api

// ExecutionDriver is the credential-free public description of a registered driver.
type ExecutionDriver struct {
	Kind              string   `json:"kind"`
	Description       string   `json:"description"`
	Status            string   `json:"status"`
	EvidenceIntegrity string   `json:"evidence_integrity"`
	CertifiedConfigs  []string `json:"certified_configs"`
}
