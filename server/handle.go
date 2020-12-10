package server

func (c *ToyKVClient) handlePing() error {
	return nil
}

func (c *ToyKVClient) handleSet() error {
	if len(c.args) != 2 {
		return ErrParams
	}
	key := string(c.args[0])
	value := string(c.args[1])
	err := c.server.store.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (c *ToyKVClient) handleGet() (string, error) {
	if len(c.args) != 1 {
		return "", ErrParams
	}
	key := string(c.args[0])
	return c.server.store.Get(key)
}

func (c *ToyKVClient) handleDel() error {
	if len(c.args) != 1 {
		return ErrParams
	}
	key := string(c.args[0])
	return c.server.store.Delete(key)
}

func (c *ToyKVClient) handleJoin() error {
	if len(c.args) != 2 {
		return ErrParams
	}

	addr := string(c.args[0])
	nodeID := string(c.args[1])
	return c.server.store.Join(nodeID, addr)
}

func (c *ToyKVClient) handleLeave() error {
	if len(c.args) != 1 {
		return ErrParams
	}
	nodeID := string(c.args[0])
	return c.server.store.Leave(nodeID)
}
