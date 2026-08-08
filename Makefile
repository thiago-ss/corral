BIN    := corral
PREFIX ?= $(HOME)/.local/bin
GO     ?= go

.PHONY: build install test test-live race vet demo clean

build: ## build the corral binary
	$(GO) build -o $(BIN) ./cmd/corral

install: build ## build and install to PREFIX (default ~/.local/bin)
	install -m 0755 $(BIN) $(PREFIX)/$(BIN)
	@echo "installed to $(PREFIX)/$(BIN)"

test: ## deterministic tests only (no opencode/model needed)
	CORRAL_LIVE=0 $(GO) test ./... -count=1 -p 1

test-live: ## full suite including real-OpenCode integration (needs a model provider)
	$(GO) test ./... -count=1 -p 1 -timeout 50m

race: ## deterministic tests under the race detector
	CORRAL_LIVE=0 $(GO) test ./... -count=1 -p 1 -race

vet: ## go vet + gofmt check
	$(GO) vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

demo: install ## one-command local demo: init + daemon + doctor
	$(PREFIX)/$(BIN) up

clean:
	rm -f $(BIN)
