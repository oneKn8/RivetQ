package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rivetq/rivetq/internal/backoff"
	"github.com/rivetq/rivetq/internal/ratelimit"
	"github.com/rivetq/rivetq/internal/store"
	"github.com/rivetq/rivetq/internal/wal"
	"github.com/rs/zerolog/log"
)

// Queue manages a single named queue
type Queue struct {
	mu sync.RWMutex

	name     string
	ready    *priorityQueue
	inflight map[string]*Job // jobID -> job
	dlq      map[string]*Job // jobID -> job

	store   *store.Store
	wal     *wal.WAL
	limiter *ratelimit.TokenBucket
}

// Manager manages multiple queues
type Manager struct {
	mu sync.RWMutex

	queues      map[string]*Queue
	store       *store.Store
	wal         *wal.WAL
	rateLimiter *ratelimit.Limiter

	// Background workers
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewManager creates a new queue manager
func NewManager(store *store.Store, wal *wal.WAL) *Manager {
	return &Manager{
		queues:      make(map[string]*Queue),
		store:       store,
		wal:         wal,
		rateLimiter: ratelimit.NewLimiter(),
		stopCh:      make(chan struct{}),
	}
}

// Start starts background workers
func (m *Manager) Start() error {
	// Replay WAL to rebuild state
	if err := m.replayWAL(); err != nil {
		return fmt.Errorf("failed to replay WAL: %w", err)
	}

	// Start lease timeout checker
	m.wg.Add(1)
	go m.leaseTimeoutWorker()

	return nil
}

// Stop stops the manager
func (m *Manager) Stop() error {
	close(m.stopCh)
	m.wg.Wait()
	return nil
}

// replayWAL replays the WAL to rebuild in-memory state
func (m *Manager) replayWAL() error {
	log.Info().Msg("replaying WAL")

	return m.wal.Replay(func(record *wal.Record) error {
		switch record.Type {
		case wal.RecordTypeEnqueue:
			queue := m.getOrCreateQueue(record.Queue)
			job := &Job{
				ID:         record.JobID,
				Queue:      record.Queue,
				Payload:    record.Payload,
				Headers:    record.Headers,
				Priority:   record.Priority,
				Tries:      record.Tries,
				MaxRetries: record.MaxRetries,
				ETA:        record.ETA,
				Status:     JobStatusReady,
				EnqueuedAt: time.Now(),
			}
			queue.ready.Push(job)

		case wal.RecordTypeAck:
			queue := m.getQueue(record.Queue)
			if queue != nil {
				queue.mu.Lock()
				delete(queue.inflight, record.JobID)
				queue.mu.Unlock()
			}

		case wal.RecordTypeNack, wal.RecordTypeRequeue:
			queue := m.getQueue(record.Queue)
			if queue != nil {
				queue.mu.Lock()
				if job, exists := queue.inflight[record.JobID]; exists {
					delete(queue.inflight, record.JobID)
					job.Tries = record.Tries
					job.ETA = record.ETA
					job.Status = JobStatusReady
					job.LeaseID = ""
					job.LeaseDeadline = time.Time{}

					if job.ShouldRetry() {
						queue.ready.Push(job)
					} else {
						job.Status = JobStatusDLQ
						queue.dlq[job.ID] = job
					}
				}
				queue.mu.Unlock()
			}

		case wal.RecordTypeTombstone:
			queue := m.getQueue(record.Queue)
			if queue != nil {
				queue.mu.Lock()
				delete(queue.inflight, record.JobID)
				delete(queue.dlq, record.JobID)
				queue.mu.Unlock()
			}
		}

		return nil
	})
}

// getOrCreateQueue gets or creates a queue
func (m *Manager) getOrCreateQueue(name string) *Queue {
	m.mu.Lock()
	defer m.mu.Unlock()

	queue, exists := m.queues[name]
	if !exists {
		queue = &Queue{
			name:     name,
			ready:    newPriorityQueue(),
			inflight: make(map[string]*Job),
			dlq:      make(map[string]*Job),
			store:    m.store,
			wal:      m.wal,
			limiter:  ratelimit.NewTokenBucket(0, 0), // No limit by default
		}
		m.queues[name] = queue
	}

	return queue
}

// getQueue gets a queue by name
func (m *Manager) getQueue(name string) *Queue {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queues[name]
}

// Enqueue adds a job to a queue
func (m *Manager) Enqueue(queueName string, payload []byte, headers map[string]string, priority uint8, delayMs int64, retryPolicy RetryPolicy, idempotencyKey string) (string, error) {
	// Check idempotency key
	if idempotencyKey != "" {
		existingJobID, err := m.store.GetIdempotencyKey(idempotencyKey)
		if err != nil {
			return "", fmt.Errorf("failed to check idempotency key: %w", err)
		}
		if existingJobID != "" {
			log.Debug().Str("job_id", existingJobID).Str("idempotency_key", idempotencyKey).Msg("idempotent request, returning existing job")
			return existingJobID, nil
		}
	}

	// Check rate limit
	if !m.rateLimiter.Allow(queueName) {
		return "", fmt.Errorf("rate limit exceeded for queue %s", queueName)
	}

	queue := m.getOrCreateQueue(queueName)

	// Create job
	jobID := uuid.New().String()
	eta := time.Now()
	if delayMs > 0 {
		eta = eta.Add(time.Duration(delayMs) * time.Millisecond)
	}

	job := &Job{
		ID:         jobID,
		Queue:      queueName,
		Payload:    payload,
		Headers:    headers,
		Priority:   priority,
		Tries:      0,
		MaxRetries: retryPolicy.MaxRetries,
		BaseDelay:  retryPolicy.BaseDelay,
		MaxDelay:   retryPolicy.MaxDelay,
		ETA:        eta,
		Status:     JobStatusReady,
		EnqueuedAt: time.Now(),
	}

	// Write to WAL
	record := &wal.Record{
		Type:       wal.RecordTypeEnqueue,
		Queue:      queueName,
		JobID:      jobID,
		Payload:    payload,
		Headers:    headers,
		Priority:   priority,
		Tries:      0,
		MaxRetries: retryPolicy.MaxRetries,
		ETA:        eta,
	}

	if err := m.wal.Write(record); err != nil {
		return "", fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Store idempotency key
	if idempotencyKey != "" {
		if err := m.store.SetIdempotencyKey(idempotencyKey, jobID); err != nil {
			log.Error().Err(err).Msg("failed to store idempotency key")
		}
	}

	// Add to ready queue
	queue.mu.Lock()
	queue.ready.Push(job)
	queue.mu.Unlock()

	log.Debug().Str("job_id", jobID).Str("queue", queueName).Uint8("priority", priority).Msg("job enqueued")
	return jobID, nil
}

// Lease leases jobs from a queue
func (m *Manager) Lease(queueName string, maxJobs int, visibilityMs int64) ([]*Job, error) {
	queue := m.getQueue(queueName)
	if queue == nil {
		return nil, fmt.Errorf("queue not found: %s", queueName)
	}

	if maxJobs <= 0 {
		maxJobs = 1
	}

	visibilityTimeout := time.Duration(visibilityMs) * time.Millisecond
	now := time.Now()
	leaseDeadline := now.Add(visibilityTimeout)

	jobs := make([]*Job, 0, maxJobs)

	queue.mu.Lock()
	defer queue.mu.Unlock()

	for i := 0; i < maxJobs; i++ {
		job := queue.ready.PopReady(now)
		if job == nil {
			break
		}

		// Generate lease ID
		leaseID := uuid.New().String()
		job.LeaseID = leaseID
		job.LeaseDeadline = leaseDeadline
		job.Status = JobStatusInflight

		// Move to inflight
		queue.inflight[job.ID] = job
		jobs = append(jobs, job)

		log.Debug().Str("job_id", job.ID).Str("lease_id", leaseID).Msg("job leased")
	}

	return jobs, nil
}

// Ack acknowledges a job completion
func (m *Manager) Ack(jobID, leaseID string) error {
	// Find the job
	var queue *Queue
	var job *Job

	m.mu.RLock()
	for _, q := range m.queues {
		q.mu.RLock()
		if j, exists := q.inflight[jobID]; exists {
			queue = q
			job = j
		}
		q.mu.RUnlock()
		if job != nil {
			break
		}
	}
	m.mu.RUnlock()

	if job == nil {
		return fmt.Errorf("job not found or not inflight: %s", jobID)
	}

	if job.LeaseID != leaseID {
		return fmt.Errorf("invalid lease ID")
	}

	// Write to WAL
	record := &wal.Record{
		Type:    wal.RecordTypeAck,
		Queue:   job.Queue,
		JobID:   jobID,
		LeaseID: leaseID,
	}

	if err := m.wal.Write(record); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Remove from inflight
	queue.mu.Lock()
	delete(queue.inflight, jobID)
	queue.mu.Unlock()

	log.Debug().Str("job_id", jobID).Msg("job acknowledged")
	return nil
}

// retryBackoff computes the requeue delay for a job, honoring its configured
// retry policy and falling back to the default config for any unset field
// (e.g. jobs restored from the WAL, which does not persist per-job delays).
func retryBackoff(job *Job) time.Duration {
	cfg := backoff.DefaultConfig()
	if job.BaseDelay > 0 {
		cfg.BaseDelay = job.BaseDelay
	}
	if job.MaxDelay > 0 {
		cfg.MaxDelay = job.MaxDelay
	}
	return backoff.Calculate(cfg, job.Tries)
}

// Nack negatively acknowledges a job (requeue with backoff or move to DLQ)
func (m *Manager) Nack(jobID, leaseID, reason string) error {
	// Find the job
	var queue *Queue
	var job *Job

	m.mu.RLock()
	for _, q := range m.queues {
		q.mu.RLock()
		if j, exists := q.inflight[jobID]; exists {
			queue = q
			job = j
		}
		q.mu.RUnlock()
		if job != nil {
			break
		}
	}
	m.mu.RUnlock()

	if job == nil {
		return fmt.Errorf("job not found or not inflight: %s", jobID)
	}

	if job.LeaseID != leaseID {
		return fmt.Errorf("invalid lease ID")
	}

	// Increment tries
	job.Tries++

	// Calculate backoff
	backoffDelay := retryBackoff(job)
	job.ETA = time.Now().Add(backoffDelay)
	job.LeaseID = ""
	job.LeaseDeadline = time.Time{}

	// Check if should retry or move to DLQ
	if job.ShouldRetry() {
		job.Status = JobStatusReady

		// Write to WAL
		record := &wal.Record{
			Type:       wal.RecordTypeNack,
			Queue:      job.Queue,
			JobID:      jobID,
			LeaseID:    leaseID,
			Reason:     reason,
			Tries:      job.Tries,
			ETA:        job.ETA,
			Priority:   job.Priority,
			MaxRetries: job.MaxRetries,
		}

		if err := m.wal.Write(record); err != nil {
			return fmt.Errorf("failed to write to WAL: %w", err)
		}

		// Move back to ready queue
		queue.mu.Lock()
		delete(queue.inflight, jobID)
		queue.ready.Push(job)
		queue.mu.Unlock()

		log.Debug().Str("job_id", jobID).Uint32("tries", job.Tries).Msg("job nacked, requeued")
	} else {
		job.Status = JobStatusDLQ

		// Write to WAL
		record := &wal.Record{
			Type:    wal.RecordTypeNack,
			Queue:   job.Queue,
			JobID:   jobID,
			LeaseID: leaseID,
			Reason:  reason,
			Tries:   job.Tries,
		}

		if err := m.wal.Write(record); err != nil {
			return fmt.Errorf("failed to write to WAL: %w", err)
		}

		// Move to DLQ
		queue.mu.Lock()
		delete(queue.inflight, jobID)
		queue.dlq[jobID] = job
		queue.mu.Unlock()

		log.Warn().Str("job_id", jobID).Uint32("tries", job.Tries).Msg("job moved to DLQ")
	}

	return nil
}

// leaseTimeoutWorker checks for expired leases and returns them to ready queue
func (m *Manager) leaseTimeoutWorker() {
	defer m.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkLeaseTimeouts()
		}
	}
}

// checkLeaseTimeouts checks for expired leases
func (m *Manager) checkLeaseTimeouts() {
	now := time.Now()

	m.mu.RLock()
	queues := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		queues = append(queues, q)
	}
	m.mu.RUnlock()

	for _, queue := range queues {
		queue.mu.Lock()

		expiredJobs := make([]*Job, 0)
		for _, job := range queue.inflight {
			if !job.LeaseDeadline.IsZero() && job.LeaseDeadline.Before(now) {
				expiredJobs = append(expiredJobs, job)
			}
		}

		for _, job := range expiredJobs {
			log.Warn().Str("job_id", job.ID).Msg("lease expired, returning to ready queue")

			job.Tries++
			backoffDelay := retryBackoff(job)
			job.ETA = now.Add(backoffDelay)
			job.LeaseID = ""
			job.LeaseDeadline = time.Time{}

			if job.ShouldRetry() {
				job.Status = JobStatusReady
				delete(queue.inflight, job.ID)
				queue.ready.Push(job)

				// Write requeue record
				record := &wal.Record{
					Type:       wal.RecordTypeRequeue,
					Queue:      job.Queue,
					JobID:      job.ID,
					Tries:      job.Tries,
					ETA:        job.ETA,
					Priority:   job.Priority,
					MaxRetries: job.MaxRetries,
				}
				m.wal.Write(record)
			} else {
				job.Status = JobStatusDLQ
				delete(queue.inflight, job.ID)
				queue.dlq[job.ID] = job
			}
		}

		queue.mu.Unlock()
	}
}

// Stats returns statistics for a queue
func (m *Manager) Stats(queueName string) (ready, inflight, dlq int, err error) {
	queue := m.getQueue(queueName)
	if queue == nil {
		return 0, 0, 0, fmt.Errorf("queue not found: %s", queueName)
	}

	queue.mu.RLock()
	defer queue.mu.RUnlock()

	return queue.ready.Len(), len(queue.inflight), len(queue.dlq), nil
}

// ListQueues returns list of all queue names
func (m *Manager) ListQueues() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.queues))
	for name := range m.queues {
		names = append(names, name)
	}
	return names
}

// SetRateLimit sets rate limit for a queue
func (m *Manager) SetRateLimit(queueName string, capacity, refillRate float64) {
	m.rateLimiter.SetRate(queueName, capacity, refillRate)
}

// GetRateLimit gets rate limit for a queue
func (m *Manager) GetRateLimit(queueName string) (capacity, refillRate float64, exists bool) {
	return m.rateLimiter.GetRate(queueName)
}
