.PHONY: build
build:
	go build -o bin/toudai-collector .

.PHONY: test
test:
	go test ./...

.PHONY: vet
vet:
	go vet ./...

# Check-only, like CI: lists unformatted files and fails if there are any.
.PHONY: fmt
fmt:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi

.PHONY: lint
lint:
	golangci-lint run ./...
