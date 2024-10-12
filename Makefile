lint:
	go vet ./...

test: lint
	go clean -testcache
	go test -v ./...

clean:
	git clean -fd