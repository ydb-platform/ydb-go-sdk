package tvm

import (
	"context"
	"fmt"

	"a.yandex-team.ru/library/go/core/log/nop"
)

func ExampleClient() {

	config := ClientConfig{
		SelfID:    100501,
		ServiceID: YdbClientID,
		Secret:    "******",
	}
	c, err := Client(config, &nop.Logger{})
	if err != nil {
		panic(err)
	}

	token, err := c.Token(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(token)
}
