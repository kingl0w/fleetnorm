BINARY  := fleetnorm
VERSION ?= 0.1.0
LDFLAGS := -s -w -X main.version=$(VERSION)
PLATFORMS := linux/amd64 darwin/arm64 windows/amd64

.PHONY: all build check test race vet fmt tidy clean run

all: fmt vet test build

#one command for CI
check:
	@out=$$(gofmt -l .); test -z "$$out" || { echo "gofmt needed:"; echo "$$out"; exit 1; }
	go vet ./...
	go test -race ./...

#cross compile every supported platform. no cgo anywhere, so this is a loop.
build:
	@mkdir -p dist
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; ext=; \
		[ "$$os" = windows ] && ext=.exe; \
		echo "  $$os/$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch \
			go build -trimpath -ldflags "$(LDFLAGS)" \
			-o dist/$(BINARY)-$$os-$$arch$$ext ./cmd/$(BINARY) || exit 1; \
	done

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

tidy:
	go mod tidy

clean:
	rm -rf dist

#replay the shipped example against the example config
run:
	go run ./cmd/$(BINARY) -config configs/example.yaml
