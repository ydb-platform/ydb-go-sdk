IMAGE_NAME=yql-antlr-parser:latest
ANTLR_VERSION=4.13.2
GRAMMAR=SQLv1Antlr4.g4
GENERATED_DIR=files
CURRENT_DIR := $(shell pwd)

.PHONY: all
all: generate

.PHONY: build-image
build-image:
	docker build -t $(IMAGE_NAME) .

.PHONY: generate
generate: build-image
	mkdir -p $(GENERATED_DIR)
	echo $(CURRENT_DIR)/files
	docker run --rm -v "$(CURRENT_DIR)/$(GENERATED_DIR)":/workspace/files $(IMAGE_NAME) \
		java -jar /antlr-${ANTLR_VERSION}-complete.jar -Dlanguage=Go -package parsing -o files SQLv1Antlr4.g4

.PHONY: clean
clean:
	rm -rf $(GENERATED_DIR)/*.go
	rm -rf $(GENERATED_DIR)/*.interp
	rm -rf $(GENERATED_DIR)/*.tokens

.PHONY: test
test:
	go test ./...

.PHONY: regenerate
regenerate: clean generate