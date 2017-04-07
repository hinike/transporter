package commitlog

import (
	"github.com/compose/transporter/message/ops"
)

const (
	offsetPos         = 0
	sizePos           = 8
	tsPos             = 12
	attrPos           = 20
	LogEntryHeaderLen = 21

	modeMask = 3
	opMask   = 28
	opShift  = 2
)

// LogEntry represents the high level representation of the message portion of each entry in the commit log.
type LogEntry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Mode      Mode
	Op        ops.Op
}

// ModeOpToByte converts the Mode and Op values into a single byte by performing bitwise operations.
// Mode is stored in bits 0 - 1
// Op is stored in bits 2 - 4
// bits 5 - 7 are currently unused
func (le LogEntry) ModeOpToByte() byte {
	return byte(int(le.Mode) | (int(le.Op) << opShift))
}

func ModeFromBytes(b []byte) Mode {
	return Mode(b[attrPos] & modeMask)
}
func OpFromBytes(b []byte) ops.Op {
	return ops.Op(b[attrPos] & opMask >> opShift)
}

// Mode is a representation of where a in the process a reader is with respect to a given namespace.
type Mode int

// currently supported Modes are Copy, Sync, and Complete
const (
	Copy Mode = iota
	Sync
	Complete
)

// NewLogFromEntry takes the LogEntry and builds the underlying []byte to be stored.
func NewLogFromEntry(le LogEntry) Log {
	keyLen := len(le.Key)
	valLen := len(le.Value)
	kvLen := keyLen + valLen + 8
	l := make([]byte, LogEntryHeaderLen+kvLen)

	Encoding.PutUint64(l[tsPos:attrPos], le.Timestamp)

	l[attrPos] = le.ModeOpToByte()

	kvPosition := LogEntryHeaderLen + 4
	Encoding.PutUint32(l[LogEntryHeaderLen:kvPosition], uint32(keyLen))
	copy(l[kvPosition:kvPosition+keyLen], le.Key)

	Encoding.PutUint32(l[kvPosition+keyLen:kvPosition+keyLen+4], uint32(valLen))
	copy(l[kvPosition+keyLen+4:], le.Value)

	Encoding.PutUint32(l[sizePos:tsPos], uint32(kvLen))
	return l
}
