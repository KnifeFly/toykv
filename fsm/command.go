package fsm

// KeyValueCommand simple key-value operation
type KeyValueCommand struct {
	Operation string `json:"op,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
}

// NewSetCommand return set command
func NewSetCommand(key, value string) *KeyValueCommand {
	return &KeyValueCommand{
		Operation: "SET",
		Key:       key,
		Value:     value,
	}
}

// NewGetCommand return set command
func NewGetCommand(key string) *KeyValueCommand {
	return &KeyValueCommand{
		Operation: "GET",
		Key:       key,
	}
}

// NewDelCommand return del command
func NewDelCommand(key string) *KeyValueCommand {
	return &KeyValueCommand{
		Operation: "DEL",
		Key:       key,
	}
}
