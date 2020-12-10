package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
	"toykv/fsm"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

var (
	// ErrNotLeader node is not leader of raft
	ErrNotLeader error = errors.New("not leader")
)

const (
	snapshotRetain = 2

	timeout = 10
)

// Store key-value store of toykv
type Store struct {
	RaftDir  string
	RaftBind string

	raft *raft.Raft
	fsm  *fsm.RaftFsm

	logger *log.Logger
}

// NewStore return new instances of toykv store service
func NewStore(dir, bind string) *Store {
	fsm := fsm.NewFSM(dir)
	return &Store{
		RaftDir:  dir,
		RaftBind: bind,
		fsm:      fsm,
		logger:   log.New(os.Stderr, "[store] ", log.LstdFlags|log.Lshortfile),
	}
}

// Init init the toykv store service
func (s *Store) Init(bootstrap bool, nodeID string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.SnapshotThreshold = 1024

	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		s.logger.Println(err.Error())
		return nil
	}

	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 10, time.Second*timeout, os.Stderr)
	if err != nil {
		s.logger.Println(err.Error())
		return err
	}

	ss, err := raft.NewFileSnapshotStore(s.RaftDir, snapshotRetain, os.Stderr)
	if err != nil {
		s.logger.Println(err.Error())
		return err
	}

	// boltDB implement log store and stable store interface
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		s.logger.Println(err.Error())
		return err
	}

	ra, err := raft.NewRaft(config, s.fsm, boltDB, boltDB, ss, transport)
	if err != nil {
		s.logger.Println(err.Error())
		return err
	}
	s.raft = ra
	s.logger.Printf("raft info: %v", ra)

	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		s.raft.BootstrapCluster(configuration)
	}

	return nil
}

// Get return the value stored in toykv store service
func (s *Store) Get(key string) (string, error) {
	return s.fsm.Get(key)
}

// Set set the value of given key
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := fsm.NewSetCommand(key, value)
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	future := s.raft.Apply(b, time.Second*timeout)
	return future.Error()
}

// Delete delete the specified key
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := fsm.NewDelCommand(key)
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	future := s.raft.Apply(b, time.Second*timeout)
	return future.Error()
}

// Join join the cluster, identified by nodeID and located at addr, to this store.
func (s *Store) Join(nodeID, address string) error {
	s.logger.Printf("receive join request for remote node %s, addr %s", nodeID, address)
	config := s.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		s.logger.Println("failed to get raft configuration")
		return err
	}

	for _, server := range config.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			err := fmt.Errorf("node %s already joined raft cluster", nodeID)
			s.logger.Println(err)
			return err
		}
	}

	future := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 0)
	if err := future.Error(); err != nil {
		return err
	}
	s.logger.Printf("node %s at %s joined successfully\n", nodeID, address)
	return nil
}

// Leave leave the cluster, identified by nodeID
func (s *Store) Leave(nodeID string) error {
	s.logger.Printf("received leave request for remote node %s", nodeID)
	config := s.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		s.logger.Println("failed to get raft configuration")
		return err
	}

	for _, server := range config.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			future := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
			if err := future.Error(); err != nil {
				return err
			}
			s.logger.Printf("node %s leaved successfully", nodeID)
			return nil
		}
	}

	s.logger.Printf("node %s not exists in raft group", nodeID)
	return nil
}

// Snapshot snapshot of toykv
func (s *Store) Snapshot() error {
	return nil
}
