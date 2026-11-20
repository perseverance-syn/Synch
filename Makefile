.PHONY: build test vet lint
build:
	go build ./...
test:
	go test ./... -race
vet:
	go vet ./...
lint:
	golangci-lint run
