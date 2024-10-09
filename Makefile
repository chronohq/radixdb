lint:
	go vet ./...

test: lint
	go test -v ./...

clean:
	git clean -fd