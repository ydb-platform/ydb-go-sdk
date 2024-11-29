#!/bin/sh

if which ydb > /dev/null 2>&1; then
    ENDPOINT=$(echo ${YDB_CONNECTION_STRING_SECURE:-$YDB_CONNECTION_STRING} | awk -F/ '{print $3}')
    DATABASE=$(echo ${YDB_CONNECTION_STRING_SECURE:-$YDB_CONNECTION_STRING} | awk -F/ '{print "/" $4}')
    CA_FILE_OPTION=""

    if [ -n "$YDB_CONNECTION_STRING_SECURE" ]; then
        CA_FILE_OPTION="--ca-file ${YDB_SSL_ROOT_CERTIFICATES_FILE}"
    fi

    ydb config profile create local \
        --endpoint "$ENDPOINT" \
        --database "$DATABASE" \
        $CA_FILE_OPTION

    ydb config profile activate local
fi
