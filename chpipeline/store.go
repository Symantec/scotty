package chpipeline

import (
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"os"
	"path"
)

func (s *SnapshotStore) add(snapshot *Snapshot) {
	if s.snapshots.Len() == 0 {
		s.snapshots.PushBack(snapshot)
		return
	}
	lastTs := s.snapshots.Back().Value.(*Snapshot).Ts
	if snapshot.Ts.Before(lastTs) {
		panic("Times added to store must be in ascending order")
	}
	s.snapshots.PushBack(snapshot)
	oldest := s.snapshots.Front().Value.(*Snapshot)
	for snapshot.Ts.Sub(oldest.Ts) > s.span {
		s.snapshots.Remove(s.snapshots.Front())
		oldest = s.snapshots.Front().Value.(*Snapshot)
	}
}

func (s *SnapshotStore) getAll() []*Snapshot {
	result := make([]*Snapshot, 0, s.snapshots.Len())
	for e := s.snapshots.Front(); e != nil; e = e.Next() {
		result = append(result, e.Value.(*Snapshot))
	}
	return result
}

func computeId(hostName string, port uint) string {
	hash := sha256.New()
	fmt.Fprintf(hash, "%s:%d", hostName, port)
	return hex.EncodeToString(hash.Sum(nil))
}

func (s *SnapshotStore) save() error {
	path := path.Join(s.dirPath, s.id)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(s.snapshots.Len()); err != nil {
		return err
	}
	for e := s.snapshots.Front(); e != nil; e = e.Next() {
		if err := encoder.Encode(e.Value); err != nil {
			return err
		}
	}
	return nil
}

func (s *SnapshotStore) load() error {
	path := path.Join(s.dirPath, s.id)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	var length int
	if err := decoder.Decode(&length); err != nil {
		return err
	}
	snapshotSlice := make([]*Snapshot, length)
	for i := 0; i < length; i++ {
		if err := decoder.Decode(&snapshotSlice[i]); err != nil {
			return err
		}
	}
	s.snapshots.Init()
	for _, current := range snapshotSlice {
		s.add(current)
	}
	return nil
}
