# Go Etcd Cron v1.3.4

This package has been based on the project [https://github.com/robfig/cron](https://github.com/robfig/cron)

[ ![Codeship Status for Scalingo/go-etcd-cron](https://app.codeship.com/projects/36ea06c0-9bc8-0135-7b7d-329e62b9d6c9/status?branch=master)](https://app.codeship.com/projects/252777)
[![GoDoc](http://godoc.org/github.com/Scalingo/go-etcd-cron?status.png)](http://godoc.org/github.com/Scalingo/go-etcd-cron)

## Goal

This package aims at implementing a distributed and fault tolerant cron in order to:

* Run an identical process on several hosts
* Each of these process instantiate a cron with the same rules
* Ensure only one of these processes executes an iteration of a job

## Etcd Initialization

By default the library creates an etcd client on `127.0.0.1:2379`

```go
c, _ := etcdcron.NewEtcdMutexBuilder(clientv3.Config{
  Endpoints: []string{"etcd-host1:2379", "etcd-host2:2379"},
})
cron, _ := etcdcron.New(WithEtcdMutexBuilder(c))
cron.AddJob(Job{
  Name: "job0",
  Rhythm: "*/2 * * * * *",
  Func: func(ctx context.Context) error {
    // Handler
  },
})
```

## Error Handling

```go
errorsHandler := func(ctx context.Context, job etcdcron.Job, err error) {
  // Do something with the error which happened during 'job'
}
etcdErrorsHandler := func(ctx context.Context, job etcdcron.Job, err error) {
  // Do something with the error which happened at the time of the execution of 'job'
  // But locking mecanism fails because of etcd error
}

cron, _ := etcdcron.New(
  WithErrorsHandler(errorsHandler),
  WithEtcdErrorsHandler(etcdErrorsHandler),
)

cron.AddJob(Job{
  Name: "job0",
  Rhythm: "*/2 * * * * *",
  Func: func(ctx context.Context) error {
    // Handler
  },
})
```

## Release a New Version

Bump new version number in `CHANGELOG.md` and `README.md`.

Commit, tag and create a new release:

```sh
version="1.3.4"

git switch --create release/${version}
git add CHANGELOG.md README.md
git commit --message="Bump v${version}"
git push --set-upstream origin release/${version}
gh pr create --reviewer=scalingo/team-ist --title "$(git log -1 --pretty=%B)"
```

Once the pull request merged, you can tag the new release.

```sh
git tag v${version}
git push origin master v${version}
gh release create v${version}
```

The title of the release should be the version number and the text of the release is the same as the changelog.
