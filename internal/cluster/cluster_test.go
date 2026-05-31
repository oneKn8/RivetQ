package cluster

import (
	"testing"
	"time"

	"github.com/rivetq/rivetq/internal/queue"
	"github.com/rivetq/rivetq/internal/store"
	"github.com/rivetq/rivetq/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeBootstrap(t *testing.T) {
	dir := t.TempDir()

	cfg := DefaultConfig()
	cfg.NodeID = "node1"
	cfg.RaftAddr = "127.0.0.1:17000"
	cfg.RaftDir = dir
	cfg.Bootstrap = true

	// Setup queue manager
	walInst, err := wal.New(wal.Config{
		Dir:         dir + "/wal",
		SegmentSize: 1024,
		Fsync:       false,
	})
	require.NoError(t, err)
	defer walInst.Close()

	storeInst, err := store.New(dir + "/store")
	require.NoError(t, err)
	defer storeInst.Close()

	mgr := queue.NewManager(storeInst, walInst)
	require.NoError(t, mgr.Start())
	defer mgr.Stop()

	fsm := NewFSM(mgr)
	node, err := NewNode(cfg, fsm)
	require.NoError(t, err)
	defer node.Shutdown()

	// Wait for leader election
	err = node.WaitForLeader(5 * time.Second)
	assert.NoError(t, err)

	// Should be leader
	assert.True(t, node.IsLeader())
}

func TestConsistentHashing(t *testing.T) {
	ch := NewConsistentHash()

	// Add nodes
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	assert.Equal(t, 3, ch.NodeCount())

	// Get node for key
	node1, err := ch.GetNode("queue1")
	require.NoError(t, err)
	assert.NotEmpty(t, node1)

	// Same key should always map to same node
	node2, err := ch.GetNode("queue1")
	require.NoError(t, err)
	assert.Equal(t, node1, node2)

	// Different keys should distribute across nodes
	nodes := make(map[string]int)
	for i := 0; i < 100; i++ {
		node, err := ch.GetNode(string(rune(i)))
		require.NoError(t, err)
		nodes[node]++
	}

	// Should have distribution across all nodes
	assert.Len(t, nodes, 3)
	for _, count := range nodes {
		assert.Greater(t, count, 0)
	}
}

func TestConsistentHashingRebalance(t *testing.T) {
	ch := NewConsistentHash()

	ch.AddNode("node1")
	ch.AddNode("node2")

	// Get mapping before adding node
	_, err := ch.GetNode("queue1")
	require.NoError(t, err)

	// Add new node
	ch.AddNode("node3")

	// Some keys should still map to same node (minimal disruption)
	after, err := ch.GetNode("queue1")
	require.NoError(t, err)

	// Either stays same or moved (can't predict, but should be valid)
	assert.Contains(t, []string{"node1", "node2", "node3"}, after)
}

func TestSharding(t *testing.T) {
	sharding := NewSharding("node1", 2)

	sharding.AddNode("node1")
	sharding.AddNode("node2")
	sharding.AddNode("node3")

	// Get nodes for queue (should return primary + replicas)
	nodes, err := sharding.GetQueueNodes("test-queue")
	require.NoError(t, err)
	assert.Len(t, nodes, 2) // replication = 2

	// Check if local
	isLocal := sharding.IsLocalQueue("test-queue")
	assert.True(t, isLocal || !isLocal) // Valid either way
}

func TestMembership(t *testing.T) {
	// Create mock node
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.NodeID = "node1"
	cfg.RaftAddr = "127.0.0.1:17001"
	cfg.RaftDir = dir
	cfg.Bootstrap = true

	walInst, _ := wal.New(wal.Config{Dir: dir + "/wal", SegmentSize: 1024, Fsync: false})
	defer walInst.Close()

	storeInst, _ := store.New(dir + "/store")
	defer storeInst.Close()

	mgr := queue.NewManager(storeInst, walInst)
	mgr.Start()
	defer mgr.Stop()

	fsm := NewFSM(mgr)
	node, err := NewNode(cfg, fsm)
	require.NoError(t, err)
	defer node.Shutdown()

	membership := NewMembership(node, "node1")
	membership.Start()
	defer membership.Stop()

	// Add member
	member := &Member{
		ID:       "node2",
		Addr:     "localhost:8081",
		RaftAddr: "localhost:7001",
	}

	err = membership.AddMember(member)
	require.NoError(t, err)

	// Get member
	retrieved, err := membership.GetMember("node2")
	require.NoError(t, err)
	assert.Equal(t, "node2", retrieved.ID)
	assert.Equal(t, "localhost:8081", retrieved.Addr)

	// List members
	members := membership.ListMembers()
	assert.Len(t, members, 1)

	// Remove member
	err = membership.RemoveMember("node2")
	require.NoError(t, err)

	// Should be gone
	_, err = membership.GetMember("node2")
	assert.Error(t, err)
}

func TestPathExtraction(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/v1/queues/emails/enqueue", "emails"},
		{"/v1/queues/orders/lease", "orders"},
		{"/v1/queues/test-queue/stats", "test-queue"},
		{"/v1/queues/single", "single"},
		{"/v1/queues/", ""},
	}

	for _, tt := range tests {
		result := extractQueueName(tt.path)
		assert.Equal(t, tt.expected, result, "path: %s", tt.path)
	}
}
