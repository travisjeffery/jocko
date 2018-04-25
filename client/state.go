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
	PartitionLeaders map[string]map[int32]*Node //topics partitions leaders indexed by topic
}

func (cs *ClusterState) updateNodesTopics(resp *protocol.MetadataResponse) {
	cs.Nodes = make(map[int32]*Node)
	for _,b := range resp.Brokers {
		n := &Node{b.NodeID,b.Host,b.Port}
		cs.Nodes[b.NodeID]=n
		if resp.ControllerID == b.NodeID {
			cs.Controller = n
		}
	}
	if len(resp.TopicMetadata)>0 {
		for _, tm := range resp.TopicMetadata {
			if len(tm.PartitionMetadata) > 0 {
				cs.PartitionLeaders = make(map[string]map[int32]*Node)
				pl := make(map[int32]*Node)
				cs.PartitionLeaders[tm.Topic]=pl
				for _, pm := range tm.PartitionMetadata {
					pl[pm.ParititionID]=cs.Nodes[pm.Leader]
				}
			}
		}
	}
}

