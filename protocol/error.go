package protocol

import "fmt"

// See https://kafka.apache.org/protocol#protocol_error_codes - for details.
const (
	ErrUnknown Error = iota - 1
	ErrNone
	ErrOffsetOutOfRange
	ErrCorruptMessage
	ErrUnknownTopicOrPartition
	ErrInvalidFetchSize
	ErrLeaderNotAvailable
	ErrNotLeaderForPartition
	ErrRequestTimedOut
	ErrBrokerNotAvailable
	ErrReplicaNotAvailable
	ErrMessageTooLarge
	ErrStaleControllerEpoch
	ErrOffsetMetadataTooLarge
	ErrNetworkException
	ErrCoordinatorLoadInProgress
	ErrCoordinatorNotAvailable
	ErrNotCoordinator
	ErrInvalidTopicException
	ErrRecordListTooLarge
	ErrNotEnoughReplicas
	ErrNotEnoughReplicasAfterAppend
	ErrInvalidRequiredAcks
	ErrIllegalGeneration
	ErrInconsistentGroupProtocol
	ErrInvalidGroupId
	ErrUnknownMemberId
	ErrInvalidSessionTimeout
	ErrRebalanceInProgress
	ErrInvalidCommitOffsetSize
	ErrTopicAuthorizationFailed
	ErrGroupAuthorizationFailed
	ErrClusterAuthorizationFailed
	ErrInvalidTimestamp
	ErrUnsupportedSaslMechanism
	ErrIllegalSaslState
	ErrUnsupportedVersion
	ErrTopicAlreadyExists
	ErrInvalidPartitions
	ErrInvalidReplicationFactor
	ErrInvalidReplicaAssignment
	ErrInvalidConfig
	ErrNotController
	ErrInvalidRequest
	ErrUnsupportedForMessageFormat
	ErrPolicyViolation
	ErrOutOfOrderSequenceNumber
	ErrDuplicateSequenceNumber
	ErrInvalidProducerEpoch
	ErrInvalidTxnState
	ErrInvalidProducerIdMapping
	ErrInvalidTransactionTimeout
	ErrConcurrentTransactions
	ErrTransactionCoordinatorFenced
	ErrTransactionalIdAuthorizationFailed
	ErrSecurityDisabled
	ErrOperationNotAttempted
)

// Error represents a protocol err. It makes it so the errors can have their
// error code and description too.
type Error int16

func (e Error) Error() string {
	switch e {
	case ErrUnknown:
		return "unknown"
	case ErrNone:
		return "none"
	case ErrOffsetOutOfRange:
		return "offset out of range"
	case ErrCorruptMessage:
		return "corrupt message"
	case ErrUnknownTopicOrPartition:
		return "unknown topic or partition"
	case ErrInvalidFetchSize:
		return "invalid fetch size"
	case ErrLeaderNotAvailable:
		return "leader not available"
	case ErrNotLeaderForPartition:
		return "not leader for partition"
	case ErrRequestTimedOut:
		return "request timed out"
	case ErrBrokerNotAvailable:
		return "broker not available"
	case ErrReplicaNotAvailable:
		return "replica not available"
	case ErrMessageTooLarge:
		return "message too large"
	case ErrStaleControllerEpoch:
		return "stale controller epoch"
	case ErrOffsetMetadataTooLarge:
		return "offset metadata too large"
	case ErrNetworkException:
		return "network exception"
	case ErrCoordinatorLoadInProgress:
		return "coordinator load in progress"
	case ErrCoordinatorNotAvailable:
		return "coordinator not available"
	case ErrNotCoordinator:
		return "not coordinator"
	case ErrInvalidTopicException:
		return "invalid topic exception"
	case ErrRecordListTooLarge:
		return "record list too large"
	case ErrNotEnoughReplicas:
		return "not enough replicas"
	case ErrNotEnoughReplicasAfterAppend:
		return "not enough replicas after append"
	case ErrInvalidRequiredAcks:
		return "invalid required acks"
	case ErrIllegalGeneration:
		return "illegal generation"
	case ErrInconsistentGroupProtocol:
		return "inconsistent group protocol"
	case ErrInvalidGroupId:
		return "invalid group id"
	case ErrUnknownMemberId:
		return "unknown member id"
	case ErrInvalidSessionTimeout:
		return "invalid session timeout"
	case ErrRebalanceInProgress:
		return "rebalance in progress"
	case ErrInvalidCommitOffsetSize:
		return "invalid commit offset size"
	case ErrTopicAuthorizationFailed:
		return "topic authorization failed"
	case ErrGroupAuthorizationFailed:
		return "group authorization failed"
	case ErrClusterAuthorizationFailed:
		return "cluster authorization failed"
	case ErrInvalidTimestamp:
		return "invalid timestamp"
	case ErrUnsupportedSaslMechanism:
		return "unsupported sasl mechanism"
	case ErrIllegalSaslState:
		return "illegal sasl state"
	case ErrUnsupportedVersion:
		return "unsupported Version"
	case ErrTopicAlreadyExists:
		return "topic already exists"
	case ErrInvalidPartitions:
		return "invalid partitions"
	case ErrInvalidReplicationFactor:
		return "invalid replication factor"
	case ErrInvalidReplicaAssignment:
		return "invalid replica assignment"
	case ErrInvalidConfig:
		return "invalid config"
	case ErrNotController:
		return "not controller"
	case ErrInvalidRequest:
		return "invalid request"
	case ErrUnsupportedForMessageFormat:
		return "unsupported for message format"
	case ErrPolicyViolation:
		return "policy violation"
	case ErrOutOfOrderSequenceNumber:
		return "out of order sequence number"
	case ErrDuplicateSequenceNumber:
		return "duplicate sequence number"
	case ErrInvalidProducerEpoch:
		return "invalid producer epoch"
	case ErrInvalidTxnState:
		return "invalid txn state"
	case ErrInvalidProducerIdMapping:
		return "invalid producer id mapping"
	case ErrInvalidTransactionTimeout:
		return "invalid transaction timeout"
	case ErrConcurrentTransactions:
		return "concurrent transactions"
	case ErrTransactionCoordinatorFenced:
		return "transaction coordinator fenced"
	case ErrTransactionalIdAuthorizationFailed:
		return "transactional id authorization failed"
	case ErrSecurityDisabled:
		return "security disabled"
	case ErrOperationNotAttempted:
		return "operation not attempted"
	default:
		return fmt.Sprintf("error code %d not bound", e)
	}
}
