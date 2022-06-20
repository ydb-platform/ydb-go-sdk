package main

import (
	"context"
	"fmt"
	"log"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

var ctx, cancel = context.WithCancel(context.TODO())

// const dbName = "/pre-prod_global/aoeb66ftj1tbt1b2eimn/cc8035oc71oh9um52mv3"
const dbName = "/local"

func main() {
	defer cancel()
	// db, err := ydb.New(ctx,
	// 	// ydb.WithDialTimeout(time.Second),
	// 	ydb.WithDatabase(dbName),
	// 	ydb.WithEndpoint("lb.cc8035oc71oh9um52mv3.ydb.mdb.cloud-preprod.yandex.net:2135"),
	// 	ydb.WithAccessTokenCredentials(os.Getenv("TOKEN")),
	// 	ydb.WithPersqueueCluster("global"),
	// )
	// errFatal(err)
	// defer db.Close(ctx)
	db, err := ydb.Open(ctx, "grpc://localhost:2136?database=local",
		ydb.WithDialTimeout(time.Second),
		ydb.WithAnonymousCredentials(),
	)
	errFatal(err)
	defer db.Close(ctx)

	list(db)

	// fabric := func(ctx context.Context, db ydb.Connection) (cluster.DiscoveryClient, error) {
	// 	return intcluster.NewPQDiscovery(db, []config.Option{
	// 		config.WithOperationCancelAfter(time.Second * 10),
	// 		config.WithOperationTimeout(time.Second * 10),
	// 	}), nil
	// }

	// pqc, err := cluster.NewCluster(ctx, fabric,
	// 	// ydb.WithDialTimeout(time.Second),
	// 	ydb.WithDatabase(dbName),
	// 	ydb.WithEndpoint("lb.cc8035oc71oh9um52mv3.ydb.mdb.cloud-preprod.yandex.net:2135"),
	// 	ydb.WithAccessTokenCredentials(os.Getenv("TOKEN")),
	// )
	// errFatal(err)

	// {
	// 	r, err := pqc.DescribeForWrite(ctx, cluster.WriteParam{
	// 		Stream:         "/yc.billing.service-cloud/canary/resharded",
	// 		MessageGroupID: "some-custom-source",
	// 	})
	// 	errFatal(err)
	// 	fmt.Printf("%#v\n", r)
	// }
	// {
	// 	r, err := pqc.DescribeForRead(ctx, cluster.ReadParam{
	// 		Stream: "/yc.billing.service-cloud/canary/resharded",
	// 	})
	// 	errFatal(err)
	// 	fmt.Printf("%#v\n", r)
	// }

	// c, err := pqc.ConnectForWrite(ctx, cluster.WriteParam{
	// 	Stream:         "/yc.billing.service-cloud/canary/resharded",
	// 	MessageGroupID: "some-custom-source",
	// })
	// errFatal(err)
	// descr(c)
	// modifying(db)
}

func errFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func list(db ydb.Connection) {
	sc := db.Scheme()
	lst, err := sc.ListDirectory(ctx, dbName+"/PQ/")
	// lst, err := sc.ListDirectory(ctx, "PQ/")
	errFatal(err)
	for _, e := range lst.Children {
		fmt.Printf("%#v\n", e)
	}
}

// func descr(db ydb.Connection) {
// 	pq := db.Persqueue()

// 	descr, err := pq.DescribeStream(ctx, "/yc.billing.service-cloud/canary/resharded")
// 	errFatal(err)
// 	fmt.Printf("\n=================\n%#v\n", descr)
// }

// func modifying(db ydb.Connection) {
// 	pq := db.Persqueue()

// 	err := pq.CreateStream(ctx, "my-cute-topic", persqueue.StreamSettings{
// 		PartitionsCount: 2,
// 		SupportedCodecs: []persqueue.Codec{persqueue.CodecRaw},
// 		RetentionPeriod: time.Hour * 12,
// 		SupportedFormat: persqueue.FormatBase,
// 	})
// 	errFatal(err)

// 	descr, err := pq.DescribeStream(ctx, "my-cute-topic")
// 	errFatal(err)
// 	fmt.Printf("%#v\n", descr)

// 	errFatal(pq.AddReadRule(ctx, "my-cute-topic", persqueue.ReadRule{
// 		Consumer:                 "some-consumer",
// 		Important:                true,
// 		Codecs:                   []persqueue.Codec{persqueue.CodecRaw},
// 		SupportedFormat:          persqueue.FormatBase,
// 		StartingMessageTimestamp: time.Now(),
// 		// ServiceType:              "dummy",
// 	}))
// }
