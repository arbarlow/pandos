package storage_test

import (
	"log"
	"testing"

	"github.com/arbarlow/pandos/storage"
	pb "github.com/coreos/etcd/raft/raftpb"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestLevelDBInterface(t *testing.T) {
	var err error
	u1 := uuid.Must(uuid.NewV4(), err)
	if err != nil {
		log.Fatal(err)
	}

	dir := "../data/test/" + u1.String()
	d, err := storage.NewLevelDB(dir)
	if err != nil {
		log.Fatal(err)
	}

	ents := []pb.Entry{
		{Index: 3, Term: 1},
		{Index: 4, Term: 2},
		{Index: 5, Term: 3},
		{Index: 6, Term: 4},
	}

	// Test appendinng and retreiving
	err = d.Append(ents)
	assert.NoError(t, err)

	es, err := d.Entries(3, 6, 0)
	assert.NoError(t, err)

	assert.Len(t, es, 4)
	assert.Equal(t, es[0].Index, uint64(3))
	assert.Equal(t, es[len(es)-1].Index, uint64(6))

	// Test term get
	tm, err := d.Term(uint64(3))
	assert.NoError(t, err)
	assert.Equal(t, tm, uint64(1))

	// Test snapshot creation
	s, err := d.Snapshot()
	assert.NoError(t, err)
	assert.Equal(t, s.Metadata.Index, uint64(6))
	assert.Equal(t, s.Metadata.Term, uint64(4))
	assert.True(t, len(s.Data) > 0)

	// Test loading of snap
	dir = "../data/test/snap/" + u1.String()
	d, err = storage.NewLevelDB(dir)
	if err != nil {
		log.Fatal(err)
	}

	err = d.LoadSnapshot(s)
	assert.NoError(t, err)

	// Test entries from snapshot
	es, err = d.Entries(3, 6, 0)
	assert.NoError(t, err)

	assert.Len(t, es, 4)
	assert.Equal(t, es[0].Index, uint64(3))
	assert.Equal(t, es[len(es)-1].Index, uint64(6))
}
