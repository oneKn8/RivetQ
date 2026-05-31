package queue

import (
	"time"
)

// Job represents a queued job
type Job struct {
	ID            string
	Queue         string
	Payload       []byte
	Headers       map[string]string
	Priority      uint8 // 0-9, higher is more important
	Tries         uint32
	MaxRetries    uint32
	BaseDelay     time.Duration // retry backoff base delay (0 = use default)
	MaxDelay      time.Duration // retry backoff max delay (0 = use default)
	ETA           time.Time     // Execute Time After
	LeaseID       string
	LeaseDeadline time.Time
	Status        JobStatus
	EnqueuedAt    time.Time
}

// JobStatus represents the current status of a job
type JobStatus string

const (
	JobStatusReady    JobStatus = "ready"
	JobStatusInflight JobStatus = "inflight"
	JobStatusDLQ      JobStatus = "dlq"
)

// RetryPolicy defines retry behavior for a job
type RetryPolicy struct {
	MaxRetries uint32
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// DefaultRetryPolicy returns the default retry policy
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries: 3,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   60 * time.Second,
	}
}

// IsReady returns true if job is ready to be leased
func (j *Job) IsReady(now time.Time) bool {
	return j.Status == JobStatusReady && (j.ETA.IsZero() || j.ETA.Before(now) || j.ETA.Equal(now))
}

// IsInflight returns true if job is currently leased
func (j *Job) IsInflight() bool {
	return j.Status == JobStatusInflight
}

// IsDLQ returns true if job is in dead letter queue
func (j *Job) IsDLQ() bool {
	return j.Status == JobStatusDLQ
}

// ShouldRetry returns true if job should be retried.
// MaxRetries is the number of retries allowed after the first attempt, so a
// job is dead-lettered once its failure count exceeds MaxRetries.
func (j *Job) ShouldRetry() bool {
	return j.Tries <= j.MaxRetries
}
