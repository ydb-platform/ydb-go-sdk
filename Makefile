gtrace:
	go build -o gtrace ./cmd/gtrace

.PHONY: examples
examples: gtrace
	PATH=$(PWD):$(PATH)	go generate ./examples/...
	go build -o pinger ./examples/pinger

.PHONY: test
test: gtrace
	PATH=$(PWD):$(PATH)	go generate ./test
	go test -v ./test

clean:
	rm -f gtrace
	rm -f pinger
