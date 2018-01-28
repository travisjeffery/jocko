package fsm

import (
	"fmt"

	"github.com/travisjeffery/jocko/broker/structs"
	"github.com/travisjeffery/jocko/log"
)

func init() {
	registerCommand(structs.RegisterNodeRequestType, (*FSM).applyRegister)
	registerCommand(structs.DeregisterNodeRequestType, (*FSM).applyDeregister)
}

func (c *FSM) applyRegister(buf []byte, index uint64) interface{} {
	var req structs.RegisterNodeRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.EnsureRegistration(index, &req); err != nil {
		c.logger.Error("ensureRegistration failed", log.Error("error", err))
		return err
	}

	return nil
}

func (c *FSM) applyDeregister(buf []byte, index uint64) interface{} {
	var req structs.DeregisterNodeRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.DeleteNode(index, req.Node.Node); err != nil {
		c.logger.Error("DeleteNode failed", log.Error("error", err))
		return err
	}

	return nil
}
