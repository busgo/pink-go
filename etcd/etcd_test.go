package etcd

import (
	"context"
	"testing"
	"time"
)

func TestCli_GetWithPrefix(t *testing.T) {

	cli, err := NewEtcdCli(&CliConfig{
		Endpoints:   []string{"127.0.0.1:2379"},
		UserName:    "",
		Password:    "",
		DialTimeout: time.Second * 5,
	})

	if err != nil {
		panic(err)
	}
	_, _ = cli.Keepalive(context.Background(), "/pink/client/user/instances/192.168.0.104", "192.168.0.104", 5)

	ws := cli.Watch("/pink/client/user/instances/192.168.0.104")
	ws2 := cli.WatchWithPrefix("/pink/client/user/instances/192.168.0.105")

	go func() {
		for {

			select {
			case ch := <-ws2.KeyChangeCh:
				t.Logf("ch2 %+v", ch)
			}
		}
	}()
	for {

		select {
		case ch := <-ws.KeyChangeCh:

			t.Logf("ch %+v", ch)
		}
	}

}
