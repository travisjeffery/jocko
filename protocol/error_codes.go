package protocol

const (
	ErrUnknown                 = int16(-1)
	ErrNone                    = int16(0)
	ErrUnknownTopicOrPartition = int16(3)
	ErrNotLeaderForPartition   = int16(6)
)
