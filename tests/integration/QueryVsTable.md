# Query vs Table: бенчмарки `database/sql`

Кратко про два бенчмарка и как смотреть сохранённые CPU/memory-профили и execution trace.

## Два бенчмарка

### `BenchmarkDatabaseSQLMock`

Файл: `database_sql_mock_bench_test.go`.

Локальный in-process mock YDB по gRPC (Discovery + Table + Query): ответы фиксированные, как для запроса вида `SELECT 42`. Реальный кластер не нужен.

Внутри прогоняются подбенчи **`QueryService`** и **`TableService`**: один и тот же `db.QueryContext(ctx, "SELECT 42")`, но коннектор собирается с `ydb.WithQueryService(true)` или `false`. Параллелизм: `1` и `100` горутин (`QueryService-1`, `TableService-100`, и т.д.). Перед замерами выполняется разогрев пула сессий.

Удобен для профилирования чистого клиента/SDK без сети до настоящего YDB.

### `BenchmarkDatabaseSQL`

Файл: `database_sql_over_query_bench_test.go` (сборка с тегом **`integration`**).

Тот же сценарий (`SELECT 42` через `database/sql`), но подключение к реальному кластеру: строка подключения из переменной окружения **`YDB_CONNECTION_STRING`**.

Имеет смысл сравнивать Query vs Table на своём окружении.

## Имена артефактов в репозитории

В каталоге могут лежать, например:

- **`mock-*.prof`**, **`mock-*.trace`** — сняты с mock-бенча.
- **`local-ydb-*.prof`**, **`local-ydb-*.trace`** — сняты с бенча против локального/реального YDB.

Суффиксы вроде `query`, `table`, `mixed` задаются при записи (отдельный прогон под Query, под Table или смешанный).

## CPU и memory (pprof)

Просмотр интерактивного UI в браузере (подставьте свой файл):

```bash
go tool pprof -http=localhost:8080 mock-cpu-query.prof
```

Для профиля памяти:

```bash
go tool pprof -http=localhost:8080 mock-mem-query.prof
```

Полезные режимы в веб-интерфейсе: flame graph, top, просмотр по функциям. Порт можно сменить, если `8080` занят.

Запись профилей при прогоне бенча (пример для mock — из корня модуля с указанием пакета):

```bash
go test ./tests/integration -run=^$ -bench=BenchmarkDatabaseSQLMock -benchmem \
  -cpuprofile=tests/integration/mock-cpu-mixed.prof \
  -memprofile=tests/integration/mock-mem-mixed.prof
```

Для интеграционного бенча к команде добавьте `-tags=integration` и задайте `YDB_CONNECTION_STRING`.

## Execution trace (`go tool trace`)

Файлы с расширением **`.trace`** — это execution trace рантайма Go (горутины, блокировки, GC, сэмплы CPU и т.д.).

```bash
go tool trace tests/integration/local-ydb-mixed.trace
```

Команда поднимет локальный сервер и выведет URL в терминале; откройте его в браузере для таймлайна и связанных представлений.

Запись при прогоне:

```bash
go test ./tests/integration -run=^$ -bench=BenchmarkDatabaseSQLMock \
  -trace=tests/integration/mock-mixed.trace
```

Для `-tags=integration` и реального кластера используйте те же флаги, что и для обычного теста этого пакета.
