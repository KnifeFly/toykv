package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/raft"
)

// RaftFsm Raft fsm
type RaftFsm struct {
	store  Store
	logger *log.Logger
}

// NewFSM return new instances of raft fsm
func NewFSM(dir string) *RaftFsm {
	os.MkdirAll(dir, 0755)

	return &RaftFsm{
		store:  NewMemStore(),
		logger: log.New(os.Stderr, "[fsm] ", log.LstdFlags|log.Lshortfile),
	}
}

// Get get the value of specified value
func (f *RaftFsm) Get(key string) (string, error) {
	v, err := f.store.Get(key)
	if err != nil {
		return "", err
	}
	return v, nil
}

// Apply applies a Raft log entry to the key-value store.
func (f *RaftFsm) Apply(l *raft.Log) interface{} {
	var c KeyValueCommand
	if err := json.Unmarshal(l.Data, &c); err != nil {
		err := fmt.Errorf("failed to unmarshal raft log")
		f.logger.Println(err.Error())
		return err
	}

	switch strings.ToUpper(c.Operation) {
	case "SET":
		return f.applySet(c.Key, c.Value)
	case "DEL":
		return f.applyDel(c.Key)
	default:
		err := fmt.Errorf("operation not support")
		f.logger.Println(err.Error())
		return err
	}
}

// applySet apply set operation
func (f *RaftFsm) applySet(key, value string) interface{} {
	f.logger.Printf("apply set %s to %s", key, value)
	return f.store.Set(key, value)
}

// applyDel apply del operation
func (f *RaftFsm) applyDel(key string) interface{} {
	f.logger.Printf("apply del %s", key)
	_, err := f.store.Delete(key)
	return err
}

// Restore Restore
func (f *RaftFsm) Restore(rc io.ReadCloser) error {
	return nil
}

// Snapshot Snapshot
func (f *RaftFsm) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}
