package protocol

// See https://kafka.apache.org/protocol#protocol_error_codes - for details.

var (
	ErrUnknown                            = Error{-1, "unknown"}
	ErrNone                               = Error{0, "none"}
	ErrOffsetOutOfRange                   = Error{1, "offset out of range"}
	ErrCorruptMessage                     = Error{2, "corrupt message"}
	ErrUnknownTopicOrPartition            = Error{3, "unknown topic or partition"}
	ErrInvalidFetchSize                   = Error{4, "invalid fetch size"}
	ErrLeaderNotAvailable                 = Error{5, "leader not available"}
	ErrNotLeaderForPartition              = Error{6, "not leader for partition"}
	ErrRequestTimedOut                    = Error{7, "request timed out"}
	ErrBrokerNotAvailable                 = Error{8, "broker not available"}
	ErrReplicaNotAvailable                = Error{9, "replica not available"}
	ErrMessageTooLarge                    = Error{10, "message too large"}
	ErrStaleControllerEpoch               = Error{11, "stale controller epoch"}
	ErrOffsetMetadataTooLarge             = Error{12, "offset metadata too large"}
	ErrNetworkException                   = Error{13, "network exception"}
	ErrCoordinatorLoadInProgress          = Error{14, "coordinator load in progress"}
	ErrCoordinatorNotAvailable            = Error{15, "coordinator not available"}
	ErrNotCoordinator                     = Error{16, "not coordinator"}
	ErrInvalidTopicException              = Error{17, "invalid topic exception"}
	ErrRecordListTooLarge                 = Error{18, "record list too large"}
	ErrNotEnoughReplicas                  = Error{19, "not enough replicas"}
	ErrNotEnoughReplicasAfterAppend       = Error{20, "not enough replicas after append"}
	ErrInvalidRequiredAcks                = Error{21, "invalid required acks"}
	ErrIllegalGeneration                  = Error{22, "illegal generation"}
	ErrInconsistentGroupProtocol          = Error{23, "inconsistent group protocol"}
	ErrInvalidGroupId                     = Error{24, "invalid group id"}
	ErrUnknownMemberId                    = Error{25, "unknown member id"}
	ErrInvalidSessionTimeout              = Error{26, "invalid session timeout"}
	ErrRebalanceInProgress                = Error{27, "rebalance in progress"}
	ErrInvalidCommitOffsetSize            = Error{28, "invalid commit offset size"}
	ErrTopicAuthorizationFailed           = Error{29, "topic authorization failed"}
	ErrGroupAuthorizationFailed           = Error{30, "group authorization failed"}
	ErrClusterAuthorizationFailed         = Error{31, "cluster authorization failed"}
	ErrInvalidTimestamp                   = Error{32, "invalid timestamp"}
	ErrUnsupportedSaslMechanism           = Error{33, "unsupported sasl mechanism"}
	ErrIllegalSaslState                   = Error{34, "illegal sasl state"}
	ErrUnsupportedVersion                 = Error{35, "unsupported version"}
	ErrTopicAlreadyExists                 = Error{36, "topic already exists"}
	ErrInvalidPartitions                  = Error{37, "invalid partitions"}
	ErrInvalidReplicationFactor           = Error{38, "invalid replication factor"}
	ErrInvalidReplicaAssignment           = Error{39, "invalid replica assignment"}
	ErrInvalidConfig                      = Error{40, "invalid config"}
	ErrNotController                      = Error{41, "not controller"}
	ErrInvalidRequest                     = Error{42, "invalid request"}
	ErrUnsupportedForMessageFormat        = Error{43, "unsupported for message format"}
	ErrPolicyViolation                    = Error{44, "policy violation"}
	ErrOutOfOrderSequenceNumber           = Error{45, "out of order sequence number"}
	ErrDuplicateSequenceNumber            = Error{46, "duplicate sequence number"}
	ErrInvalidProducerEpoch               = Error{47, "invalid producer epoch"}
	ErrInvalidTxnState                    = Error{48, "invalid txn state"}
	ErrInvalidProducerIdMapping           = Error{49, "invalid producer id mapping"}
	ErrInvalidTransactionTimeout          = Error{50, "invalid transaction timeout"}
	ErrConcurrentTransactions             = Error{51, "concurrent transactions"}
	ErrTransactionCoordinatorFenced       = Error{52, "transaction coordinator fenced"}
	ErrTransactionalIdAuthorizationFailed = Error{53, "transactional id authorization failed"}
	ErrSecurityDisabled                   = Error{54, "security disabled"}
	ErrOperationNotAttempted              = Error{55, "operation not attempted"}
)

// Error represents a protocol err. It makes it so the errors can have their
// error code and description too.
type Error struct {
	code int16
	msg  string
}

func (e Error) Code() int16 {
	return e.code
}

func (e Error) String() string {
	return e.msg
}

func (e Error) Error() string {
	return e.msg
}
