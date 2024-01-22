package etcdcron

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const ONE_SECOND = 1*time.Second + 200*time.Millisecond

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.Start(context.Background())

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.Start(context.Background())
	cron.Stop()
	cron.AddJob(Job{
		Name:   "test-stop",
		Rhythm: "* * * * * ?",
		Func: func(ctx context.Context) error {
			wg.Done()
			return nil
		},
	})

	select {
	case <-time.After(ONE_SECOND):
		// No job ran!
	case <-wait(wg):
		t.FailNow()
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-add-before-running",
		Rhythm: "* * * * * ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})
	cron.Start(context.Background())
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.Start(context.Background())
	defer cron.Stop()

	cron.AddJob(Job{
		Name:   "test-run",
		Rhythm: "* * * * * ?",
		Func: func(context.Context) error {
			wg.Done()
			return nil
		},
	})

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-snapshot-entries",
		Rhythm: "@every 2s",
		Func: func(context.Context) error {
			wg.Done()
			return nil
		},
	})
	cron.Start(context.Background())
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	select {
	case <-time.After(ONE_SECOND):
		cron.Entries()
	}

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-multiple-1",
		Rhythm: "0 0 0 1 1 ?",
		Func:   func(context.Context) error { return nil },
	})
	cron.AddJob(Job{
		Name:   "test-multiple-2",
		Rhythm: "* * * * * ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})
	cron.AddJob(Job{
		Name:   "test-multiple-3",
		Rhythm: "0 0 0 31 12 ?",
		Func:   func(context.Context) error { return nil },
	})
	cron.AddJob(Job{
		Name:   "test-multiple-4",
		Rhythm: "* * * * * ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-twice-1",
		Rhythm: "0 0 0 1 1 ?",
		Func:   func(context.Context) error { return nil },
	})
	cron.AddJob(Job{
		Name:   "test-twice-2",
		Rhythm: "0 0 0 31 12 ?",
		Func:   func(context.Context) error { return nil },
	})
	cron.AddJob(Job{
		Name:   "test-twice-3",
		Rhythm: "* * * * * ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}

	cron.AddJob(Job{
		Name:   "test-mschedule-1",
		Rhythm: "0 0 0 1 1 ?",
		Func:   func(context.Context) error { return nil },
	})
	cron.AddJob(Job{
		Name:   "test-mschedule-2",
		Rhythm: "0 0 0 31 12 ?",
		Func:   func(context.Context) error { return nil },
	})
	cron.AddJob(Job{
		Name:   "test-mschedule-3",
		Rhythm: "* * * * * ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})
	cron.Schedule(Every(time.Minute), Job{Name: "test-mschedule-4", Func: func(context.Context) error { return nil }})
	cron.Schedule(Every(time.Second), Job{Name: "test-mschedule-5", Func: func(context.Context) error { wg.Done(); return nil }})
	cron.Schedule(Every(time.Hour), Job{Name: "test-mschedule-6", Func: func(context.Context) error { return nil }})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	now := time.Now().Local()
	spec := fmt.Sprintf("%d %d %d %d %d ?",
		now.Second()+1, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-local",
		Rhythm: spec,
		Func: func(context.Context) error {
			wg.Done()
			return nil
		},
	})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}

	cron.AddJob(Job{
		Name:   "job0",
		Rhythm: "0 0 0 30 Feb ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})
	cron.AddJob(Job{
		Name:   "job1",
		Rhythm: "0 0 0 1 1 ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})
	cron.AddJob(Job{
		Name:   "job2",
		Rhythm: "* * * * * ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})
	cron.AddJob(Job{
		Name:   "job3",
		Rhythm: "1 0 0 1 1 ?",
		Func:   func(context.Context) error { wg.Done(); return nil },
	})
	cron.Schedule(Every(5*time.Second+5*time.Nanosecond), Job{
		Name: "job4",
		Func: func(context.Context) error { wg.Done(); return nil },
	})
	cron.Schedule(Every(5*time.Minute), Job{
		Name: "job5",
		Func: func(context.Context) error { wg.Done(); return nil },
	})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	var actuals []string
	for _, entry := range cron.Entries() {
		actuals = append(actuals, entry.Job.Name)
	}

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Errorf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
			t.FailNow()
		}
	}
}

// Add some jobs, try to ListJobsByPrefix
func TestListJobsByPrefix(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	defer cron.Stop()

	// Add jobs with different prefixes
	cron.AddJob(Job{Name: "prefix_test_job1", Rhythm: "* * * * * ?", Func: func(context.Context) error { wg.Done(); return nil }})
	cron.AddJob(Job{Name: "prefix_test_job2", Rhythm: "* * * * * ?", Func: func(context.Context) error { wg.Done(); return nil }})
	cron.AddJob(Job{Name: "other_job", Rhythm: "* * * * * ?", Func: func(context.Context) error { return nil }})

	cron.Start(context.Background())

	// Ensure only jobs with the specified prefix are returned
	prefixJobs := cron.ListJobsByPrefix("prefix_test")
	if len(prefixJobs) != 2 {
		t.Errorf("ListJobsByPrefix did not return the correct number of jobs. Expected: 2, Actual: %d", len(prefixJobs))
		t.FailNow()
	}

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Add a job, then DeleteJob
func TestDeleteJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	defer cron.Stop()

	cron.AddJob(Job{Name: "delete_test_job", Rhythm: "* * * * * ?", Func: func(context.Context) error { wg.Done(); return nil }})

	// Ensure the job is in the entries before deletion
	foundBeforeDeletion := false
	for _, entry := range cron.Entries() {
		if entry.Job.Name == "delete_test_job" {
			foundBeforeDeletion = true
			break
		}
	}

	if !foundBeforeDeletion {
		t.Error("Job not found in entries before deletion")
		t.FailNow()
	}

	cron.Start(context.Background())

	err = cron.DeleteJob("delete_test_job")
	if err != nil {
		t.Errorf("Error deleting job: %v", err)
		t.FailNow()
	}

	// Ensure the job is no longer in the entries after deletion
	foundAfterDeletion := false
	for _, entry := range cron.Entries() {
		if entry.Job.Name == "delete_test_job" {
			foundAfterDeletion = true
			break
		}
	}

	if foundAfterDeletion {
		t.Error("DeleteJob did not remove the job from entries")
		t.FailNow()
	}

	// Ensure the job is not triggered after deletion
	select {
	case <-time.After(ONE_SECOND):
		// This is expected since the job should not be triggered
	case <-wait(wg):
		t.Error("Job was triggered after deletion")
	}
}

// Add a job, then GetJob
func TestGetJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	defer cron.Stop()

	jobName := "get_test_job"
	cron.AddJob(Job{Name: jobName, Rhythm: "* * * * * ?", Func: func(context.Context) error { wg.Done(); return nil }})

	cron.Start(context.Background())

	job := cron.GetJob(jobName)
	if job == nil || job.Name != jobName {
		t.Error("GetJob did not return the expected job")
		t.FailNow()
	}

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// TestCron_Parallel tests that with 2 crons with the same job
// They should only execute once each job event
func TestCron_Parallel(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron1, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	defer cron1.Stop()

	cron2, err := New()
	if err != nil {
		t.Fatal("unexpected error")
	}
	defer cron2.Stop()

	job := Job{
		Name:   "test-parallel",
		Rhythm: "* * * * * ?",
		Func: func(context.Context) error {
			wg.Done()
			return nil
		},
	}
	cron1.AddJob(job)
	cron2.AddJob(job)

	cron1.Start(context.Background())
	cron2.Start(context.Background())

	select {
	case <-time.After(time.Duration(2) * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func stop(cron *Cron) chan bool {
	ch := make(chan bool)
	go func() {
		cron.Stop()
		ch <- true
	}()
	return ch
}
