// Package engine owns the versioned harness interface, capability gate and
// persisted output shapes for engine-adapter sections 1–4. The daemon owns
// routing, grants, tool policy and storage, and the supervisor owns processes.
//
// ValidateEnv refuses a brokered environment name that CredentialName flags as
// a credential: an exact listed name (including PGPASSWORD, APIKEY, API_KEY and
// DOCKER_AUTH_CONFIG), an AWS_BEARER_TOKEN_ prefix, a name containing
// _PRIVATE_KEY anywhere, or a name ending with a credential suffix (_TOKEN,
// _TOKEN_FILE, _SECRET_KEY, _API_KEY, _ACCESS_KEY, _PASSWORD, _SECRET,
// _PRIVATE_KEY, _KEY, _CREDENTIALS). Ordinary names such as PATH, HOME, GOPATH
// and GOFLAGS pass.
package engine
