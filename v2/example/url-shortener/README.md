# URL shortener

URL shortener is an application which provide make short URL and store results into YDB.

## Usage

### Running as http-server

```bash
go get -u github.com/yandex-cloud/ydb-go-sdk/v2/example/url-shortener
YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa/key/file \
url-shortener \
   -port=80
   -connect-timeout=1s \
   -database=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu \
   -endpoint=ydb.serverless.yandexcloud.net:2135
```
Open http://localhost/ in browse and use URL shortener web interface

### Running as serverless function
Yandex function needs a go module project. First you must create go.mod file.
```bash
go mod init github.com/yandex-cloud/ydb-go-sdk/v2/example/url-shortener
zip url-shortener.zip handler.go common.go go.mod
yc sls fn version create \
   --service-account-id=aje46n285h0re8nmm5u6 \
   --runtime=golang116 \
   --entrypoint=main.Handle \
   --memory=128m \
   --execution-timeout=1s \
   --environment YDB_ENDPOINT=$ydb.serverless.yandexcloud.net:2135 \
   --environment YDB_DATABASE=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu \
   --environment YDB_METADATA_CREDENTIALS=1 \
   --source-path=./url-shortener.zip \
   --function-id=d4euc5gp5614b56crpnj
```
