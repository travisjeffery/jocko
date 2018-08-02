package structs

import (
	"bytes"

	"github.com/ugorji/go/codec"
)

type MessageType uint8

const (
	RegisterNodeRequestType        MessageType = 0
	DeregisterNodeRequestType                  = 1
	RegisterTopicRequestType                   = 2
	DeregisterTopicRequestType                 = 3
	RegisterPartitionRequestType               = 4
	DeregisterPartitionRequestType             = 5
	RegisterGroupRequestType                   = 6
)

type CheckID string

const (
	SerfCheckID           CheckID = "serf health"
	SerfCheckName                 = "Serf health status"
	SerfCheckAliveOutput          = "Node alive and reachable"
	SerfCheckFailedOutput         = "Node not live or unreachable"
)

const (
	// HealthAny is special, and is used as a wild card,
	// not as a specific state.
	HealthAny      = "any"
	HealthPassing  = "passing"
	HealthWarning  = "warning"
	HealthCritical = "critical"
	HealthMaint    = "maintenance"
)

type RegisterGroupRequest struct {
	Group Group
}

type RegisterNodeRequest struct {
	Node Node
}

type DeregisterNodeRequest struct {
	Node Node
}

type RegisterTopicRequest struct {
	Topic Topic
}

type DeregisterTopicRequest struct {
	Topic Topic
}

type RegisterPartitionRequest struct {
	Partition Partition
}

type DeregisterPartitionRequest struct {
	Partition Partition
}

// msgpackHandle is a shared handle for encoding/decoding of structs
var msgpackHandle = &codec.MsgpackHandle{}

// Decode is used to encode a MsgPack object with type prefix.
func Decode(buf []byte, out interface{}) error {
	return codec.NewDecoder(bytes.NewReader(buf), msgpackHandle).Decode(out)
}

// Encode is used to encode a MsgPack object with type prefix
func Encode(t MessageType, msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	err := codec.NewEncoder(&buf, msgpackHandle).Encode(msg)
	return buf.Bytes(), err
}

type RaftIndex struct {
	CreateIndex uint64
	ModifyIndex uint64
}

// Node is used to return info about a node
type Node struct {
	ID      int32
	Node    int32
	Address string
	Check   *HealthCheck
	Meta    map[string]string
	RaftIndex
}

// NodeService is a service provided by a node
type NodeService struct {
	ID      string
	Service string
	Tags    []string
	Address string
	Port    int
	RaftIndex
}

// HealthCheck represents a single check on a given node
type HealthCheck struct {
	Node    string
	CheckID CheckID // unique check id
	Name    string  // check name
	Status  string  // current check stauts
	Output  string  // output of script runs
	RaftIndex
}

// Topic
type Topic struct {
	// ID is ID or name of the topic
	ID string
	// Topic is the name of the topic
	Topic string
	// Partitions is a map of partition IDs to slice of replicas IDs.
	Partitions map[int32][]int32
	// Config
	Config TopicConfig
	// Internal, e.g. group metadata topic
	Internal bool

	RaftIndex
}

// Partition
type Partition struct {
	// ID identifies the partition. Is here cause memdb wants the indexed field separate.
	ID        int32
	Partition int32
	// Topic is the topic this partition belongs to.
	Topic string
	// ISR is a slice of replica IDs in ISR
	ISR []int32
	// All assigned replicas
	AR []int32
	// Leader is the ID of the leader replica
	Leader int32
	// ControllerEpoch is the epoch of the controller that last updated
	// the leader and ISR info. TODO: this will probably have to change to fit better.
	ControllerEpoch int32
	LeaderEpoch     int32

	RaftIndex
}

// Member
type Member struct {
	ID         string
	Metadata   []byte
	Assignment []byte
}

type GroupState int32

const (
	GroupStatePreparingRebalance  GroupState = 0
	GroupStateCompletingRebalance GroupState = 1
	GroupStateStable              GroupState = 2
	GroupStateDead                GroupState = 3
	GroupStateEmpty               GroupState = 4
)

// Group
type Group struct {
	ID           string
	Group        string
	Coordinator  int32
	LeaderID     string
	Members      map[string]Member
	State        GroupState
	GenerationID int32

	RaftIndex
}
