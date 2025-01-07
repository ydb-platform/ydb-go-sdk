IMAGE_NAME=yql-antlr-parser:latest
ANTLR_VERSION=4.13.2
CURRENT_DIR := $(shell pwd)

.PHONY: all
all: yql sql

.PHONY: yql
yql: build-image
	docker run --rm -v "$(CURRENT_DIR)/yql":/workspace/yql $(IMAGE_NAME) \
		java -jar /antlr-${ANTLR_VERSION}-complete.jar -Dlanguage=Go -package yql -o yql YQLv1Antlr4.g4

.PHONY: sql
sql: build-image
	docker run --rm -v "$(CURRENT_DIR)/sql":/workspace/sql $(IMAGE_NAME) \
		java -jar /antlr-${ANTLR_VERSION}-complete.jar -Dlanguage=Go -package sql -o sql SQLv1Antlr4.g4


.PHONY: build-image
build-image:
	docker build -t $(IMAGE_NAME) .


.PHONY: clean_yql
clean_yql:
	rm -rf yql/*.go
	rm -rf yql/*.interp
	rm -rf yql/*.tokens

.PHONY: clean_sql
clean_sql:
	rm -rf sql/*.go
	rm -rf sql/*.interp
	rm -rf sql/*.tokens

.PHONY: clean
clean: clean_yql clean_sql


.PHONY: regenerate_yql
regenerate_yql: clean_yql yql

.PHONY: regenerate_sql
regenerate_sql: clean_sql sql

.PHONY: regenerate
regenerate: regenerate_yql regenerate_sql
