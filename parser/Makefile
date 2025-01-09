IMAGE_NAME=yql-antlr-parser:latest
ANTLR_VERSION=4.13.2
CURRENT_DIR := $(shell pwd)

.PHONY: clean yql

yql: build-image
	docker run --rm -v "$(CURRENT_DIR)/yql":/workspace/yql $(IMAGE_NAME) \
		java -jar /antlr-${ANTLR_VERSION}-complete.jar -Dlanguage=Go -package yql -o yql YQL.g4

build-image:
	docker build -t $(IMAGE_NAME) .

clean:
	rm -rf yql/*.go
	rm -rf yql/*.interp
	rm -rf yql/*.tokens
