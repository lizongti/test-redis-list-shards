package shards

import "testing"

func TestComputeShardOwnersDeterministic(t *testing.T) {
	members := []string{"node-a", "node-b", "node-c"}

	a := computeShardOwners(16, members)
	b := computeShardOwners(16, members)

	if len(a) != len(b) {
		t.Fatalf("len mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("owner mismatch at shard=%d: %q vs %q", i, a[i], b[i])
		}
	}
}

func TestRendezvousPickOwnerAlwaysMember(t *testing.T) {
	members := []string{"node-a", "node-b"}
	for shardID := 0; shardID < 50; shardID++ {
		owner := rendezvousPickOwner(shardID, members)
		if owner != "node-a" && owner != "node-b" {
			t.Fatalf("unexpected owner %q", owner)
		}
	}
}
