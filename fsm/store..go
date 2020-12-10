package fsm

import (
	"log"
	"os"
	"sync"
)

// Store interface for fsm storage
type Store interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) (bool, error)
	GetSnapshotData() <-chan *DataItem
	Close()
}

// DataItem data item for key-value storage
type DataItem struct {
	key   string
	value string
}

// MemStore key-value storage in memory
type MemStore struct {
	mu     sync.Mutex
	m      map[string]string
	logger *log.Logger
}

// NewMemStore return a mem store instance
func NewMemStore() *MemStore {
	return &MemStore{
		m:      make(map[string]string),
		logger: log.New(os.Stderr, "mem store", log.LstdFlags|log.Llongfile),
	}
}

// Get get the value of key
func (m *MemStore) Get(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.m[key], nil
}

// Set set the value of key
func (m *MemStore) Set(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key] = value
	return nil
}

// Delete delete the specified key
func (m *MemStore) Delete(key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.m, key)
	return true, nil
}

//Close noop
func (m *MemStore) Close() {
	return
}

// GetSnapshotData get the data items
func (m *MemStore) GetSnapshotData() <-chan *DataItem {
	ch := make(chan *DataItem, 1024)
	for k, v := range m.m {
		ch <- &DataItem{
			key:   k,
			value: v,
		}
	}
	return ch
}
