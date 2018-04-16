package protocol

// Protocol API keys. See: https://kafka.apache.org/protocol#protocol_api_keys
const (
	ProduceKey                 = 0
	FetchKey                   = 1
	OffsetsKey                 = 2
	MetadataKey                = 3
	LeaderAndISRKey            = 4
	StopReplicaKey             = 5
	UpdateMetadataKey          = 6
	ControlledShutdownKey      = 7
	OffsetCommitKey            = 8
	OffsetFetchKey             = 9
	FindCoordinatorKey         = 10
	JoinGroupKey               = 11
	HeartbeatKey               = 12
	LeaveGroupKey              = 13
	SyncGroupKey               = 14
	DescribeGroupsKey          = 15
	ListGroupsKey              = 16
	SaslHandshakeKey           = 17
	APIVersionsKey             = 18
	CreateTopicsKey            = 19
	DeleteTopicsKey            = 20
	DeleteRecordsKey           = 21
	InitProducerIDKey          = 22
	OffsetForLeaderEpochKey    = 23
	AddPartitionsToTxnKey      = 24
	AddOffsetsToTxnKey         = 25
	EndTxnKey                  = 26
	WriteTxnMarkersKey         = 27
	TxnOffsetCommitKey         = 28
	DescribeAclsKey            = 29
	CreateAclsKey              = 30
	DeleteAclsKey              = 31
	DescribeConfigsKey         = 32
	AlterConfigsKey            = 33
	AlterReplicaLogDirsKey     = 34
	DescribeLogDirsKey         = 35
	SaslAuthenticateKey        = 36
	CreatePartitionsKey        = 37
	CreateDelegationTokenKey   = 38
	RenewDelegationTokenKey    = 39
	ExpireDelegationTokenKey   = 40
	DescribeDelegationTokenKey = 41
	DeleteGroupsKey            = 42
)
