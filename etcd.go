package etcdcron

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

type DistributedMutex interface {
	IsOwner() clientv3.Cmp
	Key() string
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

type EtcdMutexBuilder interface {
	NewMutex(pfx string) (DistributedMutex, error)
}

type etcdMutexBuilder struct {
	*clientv3.Client
}

func NewEtcdMutexBuilder(config clientv3.Config) (EtcdMutexBuilder, error) {
	c, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	return etcdMutexBuilder{Client: c}, nil
}

func (c etcdMutexBuilder) NewMutex(pfx string) (DistributedMutex, error) {
	session, err := concurrency.NewSession(c.Client, concurrency.WithTTL(5))
	if err != nil {
		return nil, err
	}
	return concurrency.NewMutex(session, pfx), nil
}
