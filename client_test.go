package client

import (
	"github.com/busgo/pink-go/etcd"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestNewPinkClient(t *testing.T) {

	e, _ := etcd.NewEtcdCli(&etcd.CliConfig{
		Endpoints:   []string{"127.0.0.1:2379"},
		UserName:    "",
		Password:    "",
		DialTimeout: time.Second * 5,
	})
	client := NewPinkClient(e, "user")
	client.Subscribe("com.busgo.user.job.SignInDailyJob", new(UserJob))

	for {
		//t.Logf("client:%+v", client)
		time.Sleep(time.Second)
	}
}

type UserJob struct {
}

func (u *UserJob) Target() string {
	return "com.busgo.user.job.SignInDailyJob"
}
func (u *UserJob) Execute(param string) (result string, err error) {

	log.Printf("receive param:%s", param)
	n := rand.Int63() % 20
	time.Sleep(time.Second * time.Duration(n))
	return "调用成功", err
}
