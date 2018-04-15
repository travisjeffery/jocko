package protocol

// Protocol API keys. See: https://kafka.apache.org/protocol#protocol_api_keys
const (
	ProduceKey            = 0
	FetchKey              = 1
	OffsetsKey            = 2
	MetadataKey           = 3
	LeaderAndISRKey       = 4
	StopReplicaKey        = 5
	UpdateMetadataKey     = 6
	ControlledShutdownKey = 7
	OffsetCommitKey       = 8
	OffsetFetchKey        = 9
	FindCoordinatorKey    = 10
	JoinGroupKey          = 11
	HeartbeatKey          = 12
	LeaveGroupKey         = 13
	SyncGroupKey          = 14
	DescribeGroupsKey     = 15
	ListGroupsKey         = 16
	SaslHandshakeKey      = 17
	APIVersionsKey        = 18
	CreateTopicsKey       = 19
	DeleteTopicsKey       = 20
)
