.PHONY: gtrace
gtrace:
	go build -o gtrace ./cmd/gtrace

.PHONY: examples
examples: gtrace
	PATH=$(PWD):$(PATH)	go generate ./examples/...
	go build -o pinger ./examples/pinger
	go build -o buildtags ./examples/buildtags

.PHONY: test
test: gtrace
	find ./test -name '*_gtrace*' -delete
	for os in linux darwin windows; do \
		echo "--------"; \
		echo "running: GOOS=$$os go generate ./test"; \
		echo "--------"; \
		PATH=$(PWD):$(PATH)	GOOS=$$os go generate ./test; \
		if [[ $$? -ne 0 ]]; then \
			exit 1;\
		fi; \
	done; \
	for os in linux darwin; do \
		for arch in amd64 arm64; do \
			echo "--------"; \
			echo "running: GOOS=$$os GOARCH=$$arch go generate ./test"; \
			echo "--------"; \
			PATH=$(PWD):$(PATH)	GOOS=$$os GOARCH=$$arch go generate ./test; \
			if [[ $$? -ne 0 ]]; then \
				exit 1;\
			fi; \
		done; \
	done
	go test -v -tags debug ./test

clean:
	rm -f gtrace
	rm -f pinger
	find ./test -name '*_gtrace*' -delete
	find ./examples -name '*_gtrace*' -delete
