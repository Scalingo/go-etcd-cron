package etcdcron

import (
	"context"

	etcdclient "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type DistributedMutex interface {
	IsOwner() etcdclient.Cmp
	Key() string
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

type EtcdMutexBuilder interface {
	NewMutex(pfx string) (DistributedMutex, error)
}

type etcdMutexBuilder struct {
	*etcdclient.Client
}

func NewEtcdMutexBuilderFromClient(c *etcdclient.Client) (EtcdMutexBuilder, error) {
	return etcdMutexBuilder{Client: c}, nil
}

func NewEtcdMutexBuilder(config etcdclient.Config) (EtcdMutexBuilder, error) {
	c, err := etcdclient.New(config)
	if err != nil {
		return nil, err
	}
	return etcdMutexBuilder{Client: c}, nil
}

func (c etcdMutexBuilder) NewMutex(pfx string) (DistributedMutex, error) {
	// As each task iteration lock name is unique, we don't really care about unlocking it
	// So the etcd lease will last 10 minutes, it ensures that even if another server
	// clock is ill-configured (with a maximum span of 10 minutes), it won't execute the task
	// twice.
	session, err := concurrency.NewSession(c.Client, concurrency.WithTTL(60*10))
	if err != nil {
		return nil, err
	}
	return concurrency.NewMutex(session, pfx), nil
}
