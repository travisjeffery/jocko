package structs

type RaftIndex struct {
	CreateIndex uint64
	ModifyIndex uint64
}

// Node is used to return info about a node
type Node struct {
	ID      string
	Node    string
	Address string
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
	Node        string
	CheckID     string
	Status      string
	Nodes       string
	ServiceID   string
	ServiceName string
	ServiceTags []string
	RaftIndex
}
