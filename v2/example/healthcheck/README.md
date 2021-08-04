# Healthcheck

Healthcheck application provide check URLs and store results into YDB.

## Usage

### Running as application

```bash
go get -u github.com/yandex-cloud/ydb-go-sdk/v2/example/healthcheck
YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa/key/file \
healthcheck \
   -database=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu \
   -endpoint=ydb.serverless.yandexcloud.net:2135 \
   www.ya.ru google.com rampler.ru
```

### Running as serverless function
Yandex function needs a go module project. First you must create go.mod file.
```bash
go mod init github.com/yandex-cloud/ydb-go-sdk/v2/example/healthcheck
zip healthcheck.zip healthcheck.go go.mod
yc sls fn version create \
   --service-account-id=aje46n285h0re8nmm5u6 \
   --runtime=golang116 \
   --entrypoint=main.Check \
   --memory=128m \
   --execution-timeout=1s \
   --environment YDB_ENDPOINT=$ydb.serverless.yandexcloud.net:2135 \
   --environment YDB_DATABASE=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu \
   --environment HEALTHCHECK_URLS=ya.ru,google.com,rambler.ru \
   --environment YDB_METADATA_CREDENTIALS=1 \
   --source-path=./healthcheck.zip \
   --function-id=d4empp866m0b4m2gspu9
```
