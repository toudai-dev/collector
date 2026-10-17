.PHONY: build
build:
	go build -o bin/toudai-collector .

.PHONY: test
test:
	go test ./...

.PHONY: vet
vet:
	go vet ./...
