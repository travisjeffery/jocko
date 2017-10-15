package protocol

// See https://kafka.apache.org/protocol#protocol_error_codes - for details.

var (
	ErrUnknown                            = Error{code: -1, msg: "unknown"}
	ErrNone                               = Error{code: 0, msg: "none"}
	ErrOffsetOutOfRange                   = Error{code: 1, msg: "offset out of range"}
	ErrCorruptMessage                     = Error{code: 2, msg: "corrupt message"}
	ErrUnknownTopicOrPartition            = Error{code: 3, msg: "unknown topic or partition"}
	ErrInvalidFetchSize                   = Error{code: 4, msg: "invalid fetch size"}
	ErrLeaderNotAvailable                 = Error{code: 5, msg: "leader not available"}
	ErrNotLeaderForPartition              = Error{code: 6, msg: "not leader for partition"}
	ErrRequestTimedOut                    = Error{code: 7, msg: "request timed out"}
	ErrBrokerNotAvailable                 = Error{code: 8, msg: "broker not available"}
	ErrReplicaNotAvailable                = Error{code: 9, msg: "replica not available"}
	ErrMessageTooLarge                    = Error{code: 10, msg: "message too large"}
	ErrStaleControllerEpoch               = Error{code: 11, msg: "stale controller epoch"}
	ErrOffsetMetadataTooLarge             = Error{code: 12, msg: "offset metadata too large"}
	ErrNetworkException                   = Error{code: 13, msg: "network exception"}
	ErrCoordinatorLoadInProgress          = Error{code: 14, msg: "coordinator load in progress"}
	ErrCoordinatorNotAvailable            = Error{code: 15, msg: "coordinator not available"}
	ErrNotCoordinator                     = Error{code: 16, msg: "not coordinator"}
	ErrInvalidTopicException              = Error{code: 17, msg: "invalid topic exception"}
	ErrRecordListTooLarge                 = Error{code: 18, msg: "record list too large"}
	ErrNotEnoughReplicas                  = Error{code: 19, msg: "not enough replicas"}
	ErrNotEnoughReplicasAfterAppend       = Error{code: 20, msg: "not enough replicas after append"}
	ErrInvalidRequiredAcks                = Error{code: 21, msg: "invalid required acks"}
	ErrIllegalGeneration                  = Error{code: 22, msg: "illegal generation"}
	ErrInconsistentGroupProtocol          = Error{code: 23, msg: "inconsistent group protocol"}
	ErrInvalidGroupId                     = Error{code: 24, msg: "invalid group id"}
	ErrUnknownMemberId                    = Error{code: 25, msg: "unknown member id"}
	ErrInvalidSessionTimeout              = Error{code: 26, msg: "invalid session timeout"}
	ErrRebalanceInProgress                = Error{code: 27, msg: "rebalance in progress"}
	ErrInvalidCommitOffsetSize            = Error{code: 28, msg: "invalid commit offset size"}
	ErrTopicAuthorizationFailed           = Error{code: 29, msg: "topic authorization failed"}
	ErrGroupAuthorizationFailed           = Error{code: 30, msg: "group authorization failed"}
	ErrClusterAuthorizationFailed         = Error{code: 31, msg: "cluster authorization failed"}
	ErrInvalidTimestamp                   = Error{code: 32, msg: "invalid timestamp"}
	ErrUnsupportedSaslMechanism           = Error{code: 33, msg: "unsupported sasl mechanism"}
	ErrIllegalSaslState                   = Error{code: 34, msg: "illegal sasl state"}
	ErrUnsupportedVersion                 = Error{code: 35, msg: "unsupported version"}
	ErrTopicAlreadyExists                 = Error{code: 36, msg: "topic already exists"}
	ErrInvalidPartitions                  = Error{code: 37, msg: "invalid partitions"}
	ErrInvalidReplicationFactor           = Error{code: 38, msg: "invalid replication factor"}
	ErrInvalidReplicaAssignment           = Error{code: 39, msg: "invalid replica assignment"}
	ErrInvalidConfig                      = Error{code: 40, msg: "invalid config"}
	ErrNotController                      = Error{code: 41, msg: "not controller"}
	ErrInvalidRequest                     = Error{code: 42, msg: "invalid request"}
	ErrUnsupportedForMessageFormat        = Error{code: 43, msg: "unsupported for message format"}
	ErrPolicyViolation                    = Error{code: 44, msg: "policy violation"}
	ErrOutOfOrderSequenceNumber           = Error{code: 45, msg: "out of order sequence number"}
	ErrDuplicateSequenceNumber            = Error{code: 46, msg: "duplicate sequence number"}
	ErrInvalidProducerEpoch               = Error{code: 47, msg: "invalid producer epoch"}
	ErrInvalidTxnState                    = Error{code: 48, msg: "invalid txn state"}
	ErrInvalidProducerIdMapping           = Error{code: 49, msg: "invalid producer id mapping"}
	ErrInvalidTransactionTimeout          = Error{code: 50, msg: "invalid transaction timeout"}
	ErrConcurrentTransactions             = Error{code: 51, msg: "concurrent transactions"}
	ErrTransactionCoordinatorFenced       = Error{code: 52, msg: "transaction coordinator fenced"}
	ErrTransactionalIdAuthorizationFailed = Error{code: 53, msg: "transactional id authorization failed"}
	ErrSecurityDisabled                   = Error{code: 54, msg: "security disabled"}
	ErrOperationNotAttempted              = Error{code: 55, msg: "operation not attempted"}

	// Errs maps err codes to their errs.
	Errs = map[int16]Error{
		-1: ErrUnknown,
		0:  ErrNone,
		1:  ErrOffsetOutOfRange,
		2:  ErrCorruptMessage,
		3:  ErrUnknownTopicOrPartition,
		4:  ErrInvalidFetchSize,
		5:  ErrLeaderNotAvailable,
		6:  ErrNotLeaderForPartition,
		7:  ErrRequestTimedOut,
		8:  ErrBrokerNotAvailable,
		9:  ErrReplicaNotAvailable,
		10: ErrMessageTooLarge,
		11: ErrStaleControllerEpoch,
		12: ErrOffsetMetadataTooLarge,
		13: ErrNetworkException,
		14: ErrCoordinatorLoadInProgress,
		15: ErrCoordinatorNotAvailable,
		16: ErrNotCoordinator,
		17: ErrInvalidTopicException,
		18: ErrRecordListTooLarge,
		19: ErrNotEnoughReplicas,
		20: ErrNotEnoughReplicasAfterAppend,
		21: ErrInvalidRequiredAcks,
		22: ErrIllegalGeneration,
		23: ErrInconsistentGroupProtocol,
		24: ErrInvalidGroupId,
		25: ErrUnknownMemberId,
		26: ErrInvalidSessionTimeout,
		27: ErrRebalanceInProgress,
		28: ErrInvalidCommitOffsetSize,
		29: ErrTopicAuthorizationFailed,
		30: ErrGroupAuthorizationFailed,
		31: ErrClusterAuthorizationFailed,
		32: ErrInvalidTimestamp,
		33: ErrUnsupportedSaslMechanism,
		34: ErrIllegalSaslState,
		35: ErrUnsupportedVersion,
		36: ErrTopicAlreadyExists,
		37: ErrInvalidPartitions,
		38: ErrInvalidReplicationFactor,
		39: ErrInvalidReplicaAssignment,
		40: ErrInvalidConfig,
		41: ErrNotController,
		42: ErrInvalidRequest,
		43: ErrUnsupportedForMessageFormat,
		44: ErrPolicyViolation,
		45: ErrOutOfOrderSequenceNumber,
		46: ErrDuplicateSequenceNumber,
		47: ErrInvalidProducerEpoch,
		48: ErrInvalidTxnState,
		49: ErrInvalidProducerIdMapping,
		50: ErrInvalidTransactionTimeout,
		51: ErrConcurrentTransactions,
		52: ErrTransactionCoordinatorFenced,
		53: ErrTransactionalIdAuthorizationFailed,
		54: ErrSecurityDisabled,
		55: ErrOperationNotAttempted,
	}
)

// Error represents a protocol err. It makes it so the errors can have their
// error code and description too.
type Error struct {
	code int16
	msg  string
	err  error
}

func (e Error) Code() int16 {
	return e.code
}

func (e Error) String() string {
	return e.msg
}

func (e Error) Error() string {
	if e.err != nil {
		return e.msg + ": " + e.err.Error()
	}
	return e.msg
}

func (e Error) WithErr(err error) Error {
	return Error{code: e.code, msg: e.msg, err: err}
}
