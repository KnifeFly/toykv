package config

// Config store main config item
type Config struct {
	ListenAddress string
	RaftDir       string
	RaftPort      string
	Join          string
	NodeID        string
}

// NewConfig return new instance of config
func NewConfig(listen, raftDir, raftPort, nodeID string, join string) *Config {
	return &Config{
		ListenAddress: listen,
		RaftDir:       raftDir,
		RaftPort:      raftPort,
		Join:          join,
		NodeID:        nodeID,
	}
}
