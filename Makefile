.PHONY: build lint test clean

build: lint
	go build -v ./...

test: lint
	go clean -testcache
	go test -v ./...

fuzz: lint
	go clean -testcache
	go test -fuzz=FuzzPutGet -fuzztime=1m

lint:
	go vet ./...

clean:
	git clean -fd