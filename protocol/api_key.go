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
	FindCoordinatorKey
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
	DeleteRecordsKey
	InitProducerIDKey
	OffsetForLeaderEpochKey
	AddPartitionsToTxnKey
	AddOffsetsToTxnKey
	EndTxnKey
	WriteTxnMarkersKey
	TxnOffsetCommitKey
	DescribeAclsKey
	CreateAclsKey
	DeleteAclsKey
	DescribeConfigsKey
	AlterConfigsKey
	AlterReplicaLogDirsKey
	DescribeLogDirsKey
	SaslAuthenticateKey
	CreatePartitionsKey
	CreateDelegationTokenKey
	RenewDelegationTokenKey
	ExpireDelegationTokenKey
	DescribeDelegationTokenKey
	DeleteGroupsKey
)
