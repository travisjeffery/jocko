package protocol

// APIVersions is the set of Kafka APIs this broker currently advertises as
// implemented. Keep this separate from Kafka42APIVersions until the codecs and
// handlers for modern flexible versions are implemented.
var APIVersions = []APIVersion{
	{APIKey: ProduceKey, MinVersion: 0, MaxVersion: 5},
	{APIKey: FetchKey, MinVersion: 0, MaxVersion: 3},
	{APIKey: OffsetsKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: MetadataKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: LeaderAndISRKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: StopReplicaKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: FindCoordinatorKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: JoinGroupKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: HeartbeatKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: LeaveGroupKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: SyncGroupKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: DescribeGroupsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: ListGroupsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: APIVersionsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: CreateTopicsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: DeleteTopicsKey, MinVersion: 0, MaxVersion: 1},
}

// Kafka42APIVersions is the public protocol catalog from Apache Kafka 4.2.
// It is reference metadata for compatibility work, not the broker-advertised
// implementation surface.
var Kafka42APIVersions = []APIVersion{
	{APIKey: ProduceKey, MinVersion: 3, MaxVersion: 13},
	{APIKey: FetchKey, MinVersion: 4, MaxVersion: 18},
	{APIKey: ListOffsetsKey, MinVersion: 1, MaxVersion: 11},
	{APIKey: MetadataKey, MinVersion: 0, MaxVersion: 13},
	{APIKey: OffsetCommitKey, MinVersion: 2, MaxVersion: 10},
	{APIKey: OffsetFetchKey, MinVersion: 1, MaxVersion: 10},
	{APIKey: FindCoordinatorKey, MinVersion: 0, MaxVersion: 6},
	{APIKey: JoinGroupKey, MinVersion: 0, MaxVersion: 9},
	{APIKey: HeartbeatKey, MinVersion: 0, MaxVersion: 4},
	{APIKey: LeaveGroupKey, MinVersion: 0, MaxVersion: 5},
	{APIKey: SyncGroupKey, MinVersion: 0, MaxVersion: 5},
	{APIKey: DescribeGroupsKey, MinVersion: 0, MaxVersion: 6},
	{APIKey: ListGroupsKey, MinVersion: 0, MaxVersion: 5},
	{APIKey: SaslHandshakeKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: APIVersionsKey, MinVersion: 0, MaxVersion: 4},
	{APIKey: CreateTopicsKey, MinVersion: 2, MaxVersion: 7},
	{APIKey: DeleteTopicsKey, MinVersion: 1, MaxVersion: 6},
	{APIKey: DeleteRecordsKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: InitProducerIdKey, MinVersion: 0, MaxVersion: 6},
	{APIKey: OffsetForLeaderEpochKey, MinVersion: 2, MaxVersion: 4},
	{APIKey: AddPartitionsToTxnKey, MinVersion: 0, MaxVersion: 5},
	{APIKey: AddOffsetsToTxnKey, MinVersion: 0, MaxVersion: 4},
	{APIKey: EndTxnKey, MinVersion: 0, MaxVersion: 5},
	{APIKey: WriteTxnMarkersKey, MinVersion: 1, MaxVersion: 2},
	{APIKey: TxnOffsetCommitKey, MinVersion: 0, MaxVersion: 5},
	{APIKey: DescribeAclsKey, MinVersion: 1, MaxVersion: 3},
	{APIKey: CreateAclsKey, MinVersion: 1, MaxVersion: 3},
	{APIKey: DeleteAclsKey, MinVersion: 1, MaxVersion: 3},
	{APIKey: DescribeConfigsKey, MinVersion: 1, MaxVersion: 4},
	{APIKey: AlterConfigsKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: AlterReplicaLogDirsKey, MinVersion: 1, MaxVersion: 2},
	{APIKey: DescribeLogDirsKey, MinVersion: 1, MaxVersion: 4},
	{APIKey: SaslAuthenticateKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: CreatePartitionsKey, MinVersion: 0, MaxVersion: 3},
	{APIKey: CreateDelegationTokenKey, MinVersion: 1, MaxVersion: 3},
	{APIKey: RenewDelegationTokenKey, MinVersion: 1, MaxVersion: 2},
	{APIKey: ExpireDelegationTokenKey, MinVersion: 1, MaxVersion: 2},
	{APIKey: DescribeDelegationTokenKey, MinVersion: 1, MaxVersion: 3},
	{APIKey: DeleteGroupsKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: ElectLeadersKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: IncrementalAlterConfigsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: AlterPartitionReassignmentsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: ListPartitionReassignmentsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: OffsetDeleteKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: DescribeClientQuotasKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: AlterClientQuotasKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: DescribeUserScramCredentialsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: AlterUserScramCredentialsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: DescribeQuorumKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: UpdateFeaturesKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: DescribeClusterKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: DescribeProducersKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: UnregisterBrokerKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: DescribeTransactionsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: ListTransactionsKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: ConsumerGroupHeartbeatKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: ConsumerGroupDescribeKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: GetTelemetrySubscriptionsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: PushTelemetryKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: ListConfigResourcesKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: DescribeTopicPartitionsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: ShareGroupHeartbeatKey, MinVersion: 1, MaxVersion: 1},
	{APIKey: ShareGroupDescribeKey, MinVersion: 1, MaxVersion: 1},
	{APIKey: ShareFetchKey, MinVersion: 1, MaxVersion: 2},
	{APIKey: ShareAcknowledgeKey, MinVersion: 1, MaxVersion: 2},
	{APIKey: AddRaftVoterKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: RemoveRaftVoterKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: InitializeShareGroupStateKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: ReadShareGroupStateKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: WriteShareGroupStateKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: DeleteShareGroupStateKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: ReadShareGroupStateSummaryKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: StreamsGroupHeartbeatKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: StreamsGroupDescribeKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: DescribeShareGroupOffsetsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: AlterShareGroupOffsetsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: DeleteShareGroupOffsetsKey, MinVersion: 0, MaxVersion: 0},
}

func SupportedAPIVersion(apiKey, version int16) bool {
	apiVersion, ok := APIVersionForKey(apiKey, APIVersions)
	return ok && version >= apiVersion.MinVersion && version <= apiVersion.MaxVersion
}

func APIVersionForKey(apiKey int16, apiVersions []APIVersion) (APIVersion, bool) {
	for _, apiVersion := range apiVersions {
		if apiVersion.APIKey == apiKey {
			return apiVersion, true
		}
	}
	return APIVersion{}, false
}
