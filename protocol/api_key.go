package protocol

// Protocol API keys. See: https://kafka.apache.org/protocol#protocol_api_keys
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
	APIVersionsKey
	CreateTopicsKey
	DeleteTopicsKey
)
