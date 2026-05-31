package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALWriteAndReplay(t *testing.T) {
	dir := t.TempDir()

	// Create WAL
	wal, err := New(Config{
		Dir:         dir,
		SegmentSize: 1024,
		Fsync:       false,
	})
	require.NoError(t, err)
	defer wal.Close()

	// Write records
	records := []*Record{
		{
			Type:     RecordTypeEnqueue,
			Queue:    "test",
			JobID:    "job1",
			Payload:  []byte("payload1"),
			Headers:  map[string]string{"key": "value"},
			Priority: 5,
			ETA:      time.Now(),
		},
		{
			Type:    RecordTypeAck,
			Queue:   "test",
			JobID:   "job1",
			LeaseID: "lease1",
		},
	}

	for _, rec := range records {
		err := wal.Write(rec)
		require.NoError(t, err)
	}

	// Close and reopen
	wal.Close()

	wal, err = New(Config{
		Dir:         dir,
		SegmentSize: 1024,
		Fsync:       false,
	})
	require.NoError(t, err)
	defer wal.Close()

	// Replay
	var replayed []*Record
	err = wal.Replay(func(rec *Record) error {
		replayed = append(replayed, rec)
		return nil
	})
	require.NoError(t, err)

	assert.Len(t, replayed, 2)
	assert.Equal(t, RecordTypeEnqueue, replayed[0].Type)
	assert.Equal(t, "test", replayed[0].Queue)
	assert.Equal(t, "job1", replayed[0].JobID)
}

func TestWALSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	// Create WAL with small segment size
	wal, err := New(Config{
		Dir:         dir,
		SegmentSize: 100, // Very small to trigger rotation
		Fsync:       false,
	})
	require.NoError(t, err)
	defer wal.Close()

	// Write many records
	for i := 0; i < 10; i++ {
		rec := &Record{
			Type:    RecordTypeEnqueue,
			Queue:   "test",
			JobID:   "job",
			Payload: make([]byte, 50),
		}
		err := wal.Write(rec)
		require.NoError(t, err)
	}

	// Should have multiple segments
	assert.Greater(t, wal.SegmentCount(), 1)
}

func TestRecordMarshalUnmarshal(t *testing.T) {
	rec := &Record{
		Type:       RecordTypeEnqueue,
		Queue:      "test-queue",
		JobID:      "job-123",
		Payload:    []byte("test payload"),
		Headers:    map[string]string{"foo": "bar", "baz": "qux"},
		Priority:   7,
		Tries:      2,
		MaxRetries: 5,
		ETA:        time.Now().Truncate(time.Millisecond),
		LeaseID:    "lease-456",
		Reason:     "test reason",
	}

	// Marshal
	data, err := rec.Marshal()
	require.NoError(t, err)

	// Unmarshal
	rec2 := &Record{}
	err = rec2.Unmarshal(data)
	require.NoError(t, err)

	// Compare
	assert.Equal(t, rec.Type, rec2.Type)
	assert.Equal(t, rec.Queue, rec2.Queue)
	assert.Equal(t, rec.JobID, rec2.JobID)
	assert.Equal(t, rec.Payload, rec2.Payload)
	assert.Equal(t, rec.Headers, rec2.Headers)
	assert.Equal(t, rec.Priority, rec2.Priority)
	assert.Equal(t, rec.Tries, rec2.Tries)
	assert.Equal(t, rec.MaxRetries, rec2.MaxRetries)
	assert.Equal(t, rec.ETA.Unix(), rec2.ETA.Unix())
	assert.Equal(t, rec.LeaseID, rec2.LeaseID)
	assert.Equal(t, rec.Reason, rec2.Reason)
}
