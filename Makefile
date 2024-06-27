 SHELL=/usr/bin/env bash

 all: build
.PHONY: all

unexport GOFLAGS

ldflags=-X=github.com/gh-efforts/rbot/build.CurrentCommit=+git.$(subst -,.,$(shell git describe --always --match=NeVeRmAtCh --dirty 2>/dev/null || git rev-parse --short HEAD 2>/dev/null))
ifneq ($(strip $(LDFLAGS)),)
	ldflags+=-extldflags=$(LDFLAGS)
endif

GOFLAGS+=-ldflags="$(ldflags)"

build: rbot
.PHONY: build

calibnet: GOFLAGS+=-tags=calibnet
calibnet: build

rbot:
	rm -f rbot
	go build $(GOFLAGS) -o rbot ./cmd/rbot
.PHONY: rbot

clean:
	rm -f rbot
	go clean
.PHONY: clean