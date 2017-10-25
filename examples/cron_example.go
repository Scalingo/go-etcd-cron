package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/Scalingo/go-etcd-cron"
)

func main() {
	log.Println("starting")
	cron, err := etcdcron.New()
	if err != nil {
		log.Fatal("fail to create etcd-cron", err)
	}
	cron.AddJob(etcdcron.Job{
		Name:   "test",
		Rhythm: "*/4 * * * * *",
		Func: func(ctx context.Context) error {
			// Use default logging of etcd-cron
			return errors.New("Horrible Error")
		},
	})
	cron.AddJob(etcdcron.Job{
		Name:   "test-v2",
		Rhythm: "*/10 * * * * *",
		Func: func(ctx context.Context) error {
			log.Println("Every 10 seconds from", os.Getpid())
			return nil
		},
	})
	cron.Start(context.Background())
	time.Sleep(100 * time.Second)
	cron.Stop()
}
