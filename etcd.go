package etcdcron

import (
	"context"

	client "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type DistributedMutex interface {
	IsOwner() client.Cmp
	Key() string
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

type EtcdMutexBuilder interface {
	NewMutex(pfx string) (DistributedMutex, error)
}

type etcdMutexBuilder struct {
	*client.Client
}

func NewEtcdMutexBuilder(config client.Config) (EtcdMutexBuilder, error) {
	c, err := client.New(config)
	if err != nil {
		return nil, err
	}
	return etcdMutexBuilder{Client: c}, nil
}

func (c etcdMutexBuilder) NewMutex(pfx string) (DistributedMutex, error) {
	// As each task iteration lock name is unique, we don't really care about unlocking it
	// So the etcd lease will alst 10 minutes, it ensures that even if another server
	// clock is ill-configured (with a maximum span of 10 minutes), it won't execute the task
	// twice.
	session, err := concurrency.NewSession(c.Client, concurrency.WithTTL(60*10))
	if err != nil {
		return nil, err
	}
	return concurrency.NewMutex(session, pfx), nil
}
