package storage

import (
	"encoding/binary"

	"github.com/coreos/etcd/raft"
	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// LevelDB implements a simple LevelDB database on disk for use with Raft
type LevelDB struct {
	raft.Storage
	db *leveldb.DB
}

// NewLevelDB sets up a new DB for the given dir
func NewLevelDB(dir string) (*LevelDB, error) {
	db, err := leveldb.OpenFile(dir, nil)
	ldb := &LevelDB{db: db}
	return ldb, err
}

// Append writes thing to the commit log
func (l *LevelDB) Append(entries []pb.Entry) error {
	batch := new(leveldb.Batch)
	for _, e := range entries {
		k := make([]byte, 8)
		binary.LittleEndian.PutUint64(k, uint64(e.Index))
		batch.Put(k, e.Data)
	}

	return l.db.Write(batch, nil)
}

// Close closes the DB and stops all writes
func (l *LevelDB) Close() error {
	return l.db.Close()
}

func (l *LevelDB) InitialState() (pb.HardState, pb.ConfState, error) {
	panic("not implemented")
}

// Entries returns a range of k,v for keys lo and hi
func (l *LevelDB) Entries(lo uint64, hi uint64, maxSize uint64) ([]pb.Entry, error) {
	startKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(startKey, uint64(lo))

	endKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(endKey, uint64(hi+1))

	entries := []pb.Entry{}

	iter := l.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)
	for iter.Next() {
		e := pb.Entry{}
		err := proto.Unmarshal(iter.Value(), &e)
		if err != nil {
			return entries, err
		}

		entries = append(entries, e)
	}
	iter.Release()

	err := iter.Error()
	return entries, err
}

func (l *LevelDB) Term(i uint64) (uint64, error) {
	panic("not implemented")
}

func (l *LevelDB) LastIndex() (uint64, error) {
	panic("not implemented")
}

func (l *LevelDB) FirstIndex() (uint64, error) {
	panic("not implemented")
}

func (l *LevelDB) Snapshot() (pb.Snapshot, error) {
	panic("not implemented")
}
