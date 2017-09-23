package protocol

// Supported versions.

const (
	ProduceMaxVersion = 2
	ProduceMinVersion = 2

	FetchMaxVersion = 1
	FetchMinVersion = 1

	OffsetsMaxVersion = 0
	OffsetsMinVersion = 0

	MetadataMaxVersion = 0
	MetadataMinVersion = 0

	LeaderAndISRMaxVersion = 0
	LeaderAndISRMinVersion = 0

	StopReplicaMaxVersion = 0
	StopReplicaMinVersion = 0

	// TODO: add these when supported
	// UpdateMetadataMaxVersion = 0
	// UpdateMetadataMinVersion = 0

	// ControlledShutdownMaxVersion = 0
	// ControlledShutdownMinVersion = 0

	// OffsetCommitMaxVersion = 0
	// OffsetCommitMinVersion = 0

	// OffsetFetchMaxVersion = 0
	// OffsetFetchMinVersion = 0

	GroupCoordinatorMaxVersion = 0
	GroupCoordinatorMinVersion = 0

	JoinGroupMaxVersion = 0
	JoinGroupMinVersion = 0

	HeartbeatMaxVersion = 0
	HeartbeatMinVersion = 0

	LeaveGroupMaxVersion = 0
	LeaveGroupMinVersion = 0

	SyncGroupMaxVersion = 0
	SyncGroupMinVersion = 0

	DescribeGroupsMaxVersion = 0
	DescribeGroupsMinVersion = 0

	ListGroupsMaxVersion = 0
	ListGroupsMinVersion = 0

	SaslHandshakeMaxVersion = 0
	SaslHandshakeMinVersion = 0

	APIVersionsMaxVersion = 0
	APIVersionsMinVersion = 0

	CreateTopicsMaxVersion = 0
	CreateTopicsMinVersion = 0

	DeleteTopicsMaxVersion = 0
	DeleteTopicsMinVersion = 0
)
