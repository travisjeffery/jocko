package client
import (
	"github.com/travisjeffery/jocko/protocol"
)

//cache of discovered cluster state, for clients performance
//it should be reset/rediscovered when any operations based on it failed
type ClusterState struct {
	Controller *Node  //controller broker addr
	Nodes map[int32]*Node //nodes addresses
	GroupIds map[string][]string  //groups indexed by coordinator nodes
}

func (cs *ClusterState) updateNodes(resp *protocol.MetadataResponse) {
	cs.Nodes = make(map[int32]*Node)
	for _,b := range resp.Brokers {
		n := &Node{b.NodeID,b.Host,b.Port}
		cs.Nodes[b.NodeID]=n
		if resp.ControllerID == b.NodeID {
			cs.Controller = n
		}
	}
}

