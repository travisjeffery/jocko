package protocol

const (
	ProduceKey = iota
	FetchKey
	OffsetsKey
	MetadataKey
	LeaderAndISRKey
	StopReplicaKey
	UpdateMetadataKey
	ControlledShutdownKey
	OffsetCommitKey
	OffsetFetchKey
	GroupCoordinatorKey
	JoinGroupKey
	HeartbeatKey
	LeaveGroupKey
	SyncGroupKey
	DescribeGroupsKey
	ListGroupsKey
	SaslHandshakeKey
	ApiVersionsKey
	CreateTopicsKey
	DeleteTopicsKey
)
