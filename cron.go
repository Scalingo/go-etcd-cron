package etcdcron

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	etcdclient "go.etcd.io/etcd/client/v3"
)

const (
	defaultEtcdEndpoint = "127.0.0.1:2379"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries           []*Entry
	stop              chan struct{}
	add               chan *Entry
	snapshot          chan []*Entry
	etcdErrorsHandler func(context.Context, Job, error)
	errorsHandler     func(context.Context, Job, error)
	funcCtx           func(context.Context, Job) context.Context
	running           bool
	etcdclient        EtcdMutexBuilder
}

// Job contains 3 mandatory options to define a job
type Job struct {
	// Name of the job
	Name string
	// Cron-formatted rhythm (ie. 0,10,30 1-5 0 * * *)
	Rhythm string
	// Routine method
	Func func(context.Context) error
}

func (j Job) Run(ctx context.Context) error {
	return j.Func(ctx)
}

var (
	nonAlphaNumerical = regexp.MustCompile("[^a-z0-9_]")
)

func (j Job) canonicalName() string {
	return strcase.ToSnake(
		nonAlphaNumerical.ReplaceAllString(
			strings.ToLower(j.Name),
			"_",
		),
	)
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job o run.
	Job Job
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

type CronOpt func(cron *Cron)

func WithEtcdErrorsHandler(f func(context.Context, Job, error)) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.etcdErrorsHandler = f
	})
}

func WithErrorsHandler(f func(context.Context, Job, error)) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.errorsHandler = f
	})
}

func WithEtcdMutexBuilder(b EtcdMutexBuilder) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.etcdclient = b
	})
}

func WithFuncCtx(f func(context.Context, Job) context.Context) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.funcCtx = f
	})
}

// New returns a new Cron job runner.
func New(opts ...CronOpt) (*Cron, error) {
	cron := &Cron{
		entries:  nil,
		add:      make(chan *Entry),
		stop:     make(chan struct{}),
		snapshot: make(chan []*Entry),
		running:  false,
	}
	for _, opt := range opts {
		opt(cron)
	}
	if cron.etcdclient == nil {
		etcdClient, err := NewEtcdMutexBuilder(etcdclient.Config{
			Endpoints: []string{defaultEtcdEndpoint},
		})
		if err != nil {
			return nil, err
		}
		cron.etcdclient = etcdClient
	}
	if cron.etcdErrorsHandler == nil {
		cron.etcdErrorsHandler = func(ctx context.Context, j Job, err error) {
			log.Printf("[etcd-cron] etcd error when handling '%v' job: %v", j.Name, err)
		}
	}
	if cron.errorsHandler == nil {
		cron.errorsHandler = func(ctx context.Context, j Job, err error) {
			log.Printf("[etcd-cron] error when handling '%v' job: %v", j.Name, err)
		}
	}
	return cron, nil
}

// GetJob retrieves a job by name.
func (c *Cron) GetJob(jobName string) *Job {
	for _, entry := range c.entries {
		if entry.Job.Name == jobName {
			return &entry.Job
		}
	}
	return nil
}

// AddFunc adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(job Job) error {
	schedule, err := Parse(job.Rhythm)
	if err != nil {
		return err
	}
	c.Schedule(schedule, job)
	return nil
}

// DeleteJob deletes a job by name.
func (c *Cron) DeleteJob(jobName string) error {
	var updatedEntries []*Entry
	found := false
	for _, entry := range c.entries {
		if entry.Job.Name == jobName {
			found = true
			continue
		}
		// Keep the entries that don't match the specified jobName
		updatedEntries = append(updatedEntries, entry)
	}
	if !found {
		return fmt.Errorf("job not found: %s", jobName)
	}
	c.entries = updatedEntries
	return nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, job Job) {
	entry := &Entry{
		Schedule: schedule,
		Job:      job,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
		return
	}

	c.add <- entry
}

// ListJobsByPrefix returns the list of jobs with the relevant prefix
func (c *Cron) ListJobsByPrefix(prefix string) []*Job {
	var prefixJobs []*Job
	for _, entry := range c.entries {
		if strings.HasPrefix(entry.Job.Name, prefix) {
			// Job belongs to the specified prefix
			prefixJobs = append(prefixJobs, &entry.Job)
		}
	}
	return prefixJobs
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start(ctx context.Context) {
	c.running = true
	go c.run(ctx)
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(ctx context.Context) {
	// Figure out the next activation times for each entry.
	now := time.Now().Local()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var effective time.Time
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = c.entries[0].Next
		}

		select {
		case now = <-time.After(effective.Sub(now)):
			// Run every entry whose next time was this effective time.
			for _, e := range c.entries {
				if e.Next != effective {
					break
				}
				e.Prev = e.Next
				e.Next = e.Schedule.Next(effective)

				go func(ctx context.Context, e *Entry) {
					defer func() {
						r := recover()
						if r != nil {
							err, ok := r.(error)
							if !ok {
								err = fmt.Errorf("%v", r)
							}
							err = fmt.Errorf("panic: %v, stacktrace: %s", err, string(debug.Stack()))
							go c.errorsHandler(ctx, e.Job, err)
						}
					}()

					if c.funcCtx != nil {
						ctx = c.funcCtx(ctx, e.Job)
					}

					m, err := c.etcdclient.NewMutex(fmt.Sprintf("etcd_cron/%s/%d", e.Job.canonicalName(), effective.Unix()))
					if err != nil {
						go c.etcdErrorsHandler(ctx, e.Job, errors.Wrapf(err, "fail to create etcd mutex for job '%v'", e.Job.Name))
						return
					}
					lockCtx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()

					err = m.Lock(lockCtx)
					if err == context.DeadlineExceeded {
						return
					} else if err != nil {
						go c.etcdErrorsHandler(ctx, e.Job, errors.Wrapf(err, "fail to lock mutex '%v'", m.Key()))
						return
					}

					err = e.Job.Run(ctx)
					if err != nil {
						go c.errorsHandler(ctx, e.Job, err)
						return
					}
				}(ctx, e)
			}
			continue

		case newEntry := <-c.add:
			c.entries = append(c.entries, newEntry)
			newEntry.Next = newEntry.Schedule.Next(now)

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case <-c.stop:
			return
		}

		// 'now' should be updated after newEntry and snapshot cases.
		now = time.Now().Local()
	}
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}
