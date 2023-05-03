package main

import (
	"bytes"
	"context"
	"log"
	"text/template"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type row struct {
	id          uint64
	orderID     uint64
	date        time.Time
	description string
}

func dropTableIfExists(ctx context.Context, c table.Client, path string) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.DropTable(ctx, path)
		},
		table.WithIdempotent(),
	)
	if !ydb.IsOperationErrorSchemeError(err) {
		return err
	}
	return nil
}

func createTable(ctx context.Context, c table.Client, path string) (err error) {
	return c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path,
				options.WithColumn("customer_id", types.Optional(types.TypeUint64)),
				options.WithColumn("order_id", types.Optional(types.TypeUint64)),
				options.WithColumn("order_date", types.Optional(types.TypeDate)),
				options.WithColumn("description", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("customer_id", "order_id"),
			)
		},
		table.WithIdempotent(),
	)
}

var (
	fillQuery = template.Must(template.New("fill orders").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

DECLARE $ordersData AS List<Struct<
	customer_id: Uint64,
	order_id: Uint64,
	order_date: Date,
	description: Text>>;

REPLACE INTO orders
SELECT
	customer_id,
	order_id,
	order_date,
	description
FROM AS_TABLE($ordersData);
`))
	dateLayout = "2006-01-02"
)

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

type templateConfig struct {
	TablePathPrefix string
}

func fillTable(ctx context.Context, c table.Client, prefix string) (err error) {
	return c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				table.TxControl(
					table.BeginTx(
						table.WithSerializableReadWrite(),
					),
					table.CommitTx(),
				),
				render(fillQuery, templateConfig{
					TablePathPrefix: prefix,
				}),
				table.NewQueryParameters(
					table.ValueParam(
						"$ordersData",
						types.ListValue(
							order(1, 1, "Order 1", "2006-02-03"),
							order(1, 2, "Order 2", "2007-08-24"),
							order(1, 3, "Order 3", "2008-11-21"),
							order(1, 4, "Order 4", "2010-06-25"),
							order(2, 1, "Order 1", "2014-04-06"),
							order(2, 2, "Order 2", "2015-04-12"),
							order(2, 3, "Order 3", "2016-04-24"),
							order(2, 4, "Order 4", "2017-04-23"),
							order(2, 5, "Order 5", "2018-03-25"),
							order(3, 1, "Order 1", "2019-04-23"),
							order(3, 2, "Order 3", "2020-03-25"),
						),
					),
				),
			)
			return err
		},
	)
}

func order(customerID uint64, orderID uint64, description string, date string) types.Value {
	orderDate, err := time.Parse(dateLayout, date)
	if err != nil {
		panic(err)
	}
	return types.StructValue(
		types.StructFieldValue("customer_id", types.Uint64Value(customerID)),
		types.StructFieldValue("order_id", types.Uint64Value(orderID)),
		types.StructFieldValue("order_date", types.DateValueFromTime(orderDate)),
		types.StructFieldValue("description", types.TextValue(description)),
	)
}

func readTable(ctx context.Context, c table.Client, path string, opts ...options.ReadTableOption) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			res, err := s.StreamReadTable(ctx, path, opts...)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			r := row{}
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					if res.CurrentResultSet().ColumnCount() == 4 {
						err = res.ScanNamed(
							named.OptionalWithDefault("customer_id", &r.id),
							named.OptionalWithDefault("order_id", &r.orderID),
							named.OptionalWithDefault("order_date", &r.date),
							named.OptionalWithDefault("description", &r.description),
						)
						if err != nil {
							return err
						}
						log.Printf("#  Order, CustomerId: %d, OrderId: %d, Description: %s, Order date: %s",
							r.id, r.orderID, r.description, r.date.Format("2006-01-02"),
						)
					} else {
						err = res.ScanNamed(
							named.OptionalWithDefault("customer_id", &r.id),
							named.OptionalWithDefault("order_id", &r.orderID),
							named.OptionalWithDefault("order_date", &r.date),
						)
						if err != nil {
							return err
						}
						log.Printf("#  Order, CustomerId: %d, OrderId: %d, Order date: %s", r.id, r.orderID, r.date.Format("2006-01-02"))
					}
				}
			}
			return res.Err()
		},
	)
	return err
}
