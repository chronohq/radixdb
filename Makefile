.PHONY: build lint test clean

build: lint
	go build -v ./...

test: lint
	go clean -testcache
	go test -v ./...

lint:
	go vet ./...

clean:
	git clean -fd