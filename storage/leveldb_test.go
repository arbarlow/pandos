package storage_test

import (
	"log"
	"testing"

	"github.com/arbarlow/pandos/storage"
	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/sanity-io/litter"
	uuid "github.com/satori/go.uuid"
)

func TestLevelDB(t *testing.T) {
	u1 := uuid.Must(uuid.NewV4())
	dir := "../data/test/" + u1.String()
	d, err := storage.NewLevelDB(dir)
	if err != nil {
		log.Fatal(err)
	}

	ents := []pb.Entry{
		{Index: 3, Term: 3},
		{Index: 4, Term: 4},
		{Index: 5, Term: 5},
		{Index: 6, Term: 6},
	}

	err = d.Append(ents)
	if err != nil {
		log.Fatal(err)
	}

	es, err := d.Entries(3, 6, 0)
	if err != nil {
		log.Fatal(err)
	}
	litter.Dump(es)
}
