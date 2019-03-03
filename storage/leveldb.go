// This is leveldb basic implementation and is the first implementation
// of storage. It is NOT designed to be super performant, or memory effecient,
// but should be fairly fast, and be useful for testing in the meantime without
// the heavier dependencies of RocksDB

package storage

import (
	"encoding/binary"
	"errors"

	"github.com/arbarlow/pandos/pandos"
	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const hardStateKey = -1
const confStateKey = -2

// LevelDB implements a simple LevelDB database on disk for use with Raft
type LevelDB struct {
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
		b, err := proto.Marshal(&e)
		if err != nil {
			return err
		}

		batch.Put(k, b)
	}

	return l.db.Write(batch, nil)
}

// Close closes the DB and stops all writes
func (l *LevelDB) Close() error {
	return l.db.Close()
}

// SetHardState saves the current HardState.
func (l *LevelDB) SetHardState(st pb.HardState) error {
	hsb, err := proto.Marshal(&st)
	if err != nil {
		return err
	}

	k := make([]byte, 8)
	binary.PutVarint(k, hardStateKey)
	return l.db.Put(k, hsb, nil)
}

// SetConfState saves the current ConfigState.
func (l *LevelDB) SetConfState(st pb.ConfState) error {
	cfb, err := proto.Marshal(&st)
	if err != nil {
		return err
	}

	k := make([]byte, 8)
	binary.PutVarint(k, confStateKey)
	return l.db.Put(k, cfb, nil)
}

// InitialState returns the initial state of the db
func (l *LevelDB) InitialState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}

// Entries returns a range of k,v for keys lo and hi
func (l *LevelDB) Entries(lo uint64, hi uint64, maxSize uint64) ([]pb.Entry, error) {
	startKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(startKey, uint64(lo))

	endKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(endKey, uint64(hi+1))

	entries := []pb.Entry{}

	iter := l.db.NewIterator(&util.Range{Start: startKey, Limit: endKey}, nil)
	defer iter.Release()

	for iter.Next() {
		e := pb.Entry{}
		err := proto.Unmarshal(iter.Value(), &e)
		if err != nil {
			return entries, err
		}

		entries = append(entries, e)
	}

	err := iter.Error()
	return entries, err
}

// Term returns the term for given index
func (l *LevelDB) Term(i uint64) (uint64, error) {
	k := make([]byte, 8)
	binary.LittleEndian.PutUint64(k, uint64(i))
	data, err := l.db.Get(k, nil)
	if err != nil {
		return 0, err
	}

	e := pb.Entry{}
	err = proto.Unmarshal(data, &e)
	if err != nil {
		return 0, err
	}

	return e.Term, nil
}

// LastIndex returns the last index in the DB
func (l *LevelDB) LastIndex() (uint64, error) {
	iter := l.db.NewIterator(&util.Range{Start: nil, Limit: nil}, nil)
	defer iter.Release()
	iter.Last()
	iter.Prev()

	for iter.Next() {
		key := binary.LittleEndian.Uint64(iter.Key())
		return key, nil
	}

	return 0, errors.New("LastIndex: No keys in DB")
}

// FirstIndex returns the last index in the DB
func (l *LevelDB) FirstIndex() (uint64, error) {
	iter := l.db.NewIterator(&util.Range{Start: nil, Limit: nil}, nil)
	defer iter.Release()

	for iter.Next() {
		key := binary.LittleEndian.Uint64(iter.Key())
		return key, nil
	}

	return 0, errors.New("LastIndex: No keys in DB")
}

// Snapshot makes a copy of the DB in it's current state
// for transport over the wire
func (l *LevelDB) Snapshot() (pb.Snapshot, error) {
	s, err := l.db.GetSnapshot()
	if err != nil {
		return pb.Snapshot{}, err
	}

	// Fetch all keys
	iter := s.NewIterator(&util.Range{Start: nil, Limit: nil}, nil)
	defer iter.Release()

	entries := []*pb.Entry{}

	for iter.Next() {
		e := &pb.Entry{}
		err := proto.Unmarshal(iter.Value(), e)
		if err != nil {
			return pb.Snapshot{}, err
		}

		entries = append(entries, e)
	}

	err = iter.Error()
	if err != nil {
		return pb.Snapshot{}, err
	}

	sd := pandos.SnapshotData{}
	sd.Entries = entries

	b, err := proto.Marshal(&sd)
	if err != nil {
		return pb.Snapshot{}, err
	}

	lastIndex := uint64(0)
	lastTerm := uint64(0)
	if len(entries) > 0 {
		lastIndex = entries[len(entries)-1].Index
		lastTerm = entries[len(entries)-1].Term
	}

	snap := pb.Snapshot{
		Data: b,
		Metadata: pb.SnapshotMetadata{
			Term:  lastTerm,
			Index: lastIndex,
		},
	}

	return snap, err
}

// LoadSnapshot loads a snapshot ready for use
func (l *LevelDB) LoadSnapshot(s pb.Snapshot) error {
	sd := pandos.SnapshotData{}
	err := proto.Unmarshal(s.Data, &sd)
	if err != nil {
		return err
	}

	// Proto uses pointers, so here we deference
	ent := make([]pb.Entry, len(sd.Entries))
	for k, v := range sd.Entries {
		ent[k] = *v
	}

	return l.Append(ent)
}
