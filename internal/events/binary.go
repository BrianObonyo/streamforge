package events

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Compact binary wire format ("sfb1").
//
// Profiling showed JSON decoding dominated per-event cost (~3.2 µs of ~9 µs), so
// StreamForge also speaks a dependency-free binary encoding. It is NOT protobuf:
// a hand-rolled layout keeps the core stdlib-only (see ADR-0005) and this schema is
// tiny and stable. Consumers sniff the first byte, so JSON and binary producers
// can share a topic during a migration — DecodeAny handles both.
//
// Layout (all integers little-endian unless noted):
//
//	byte    magic  = 0xB1                  ('{' = 0x7B can never collide: JSON starts with '{')
//	byte    kind   = 1 iot | 2 clickstream | 3 market
//	uvarint len, bytes   id
//	uvarint len, bytes   key
//	8 bytes float64 bits value
//	8 bytes int64 unix-nanos event_time    (math.MinInt64 = unset)
//	8 bytes int64 unix-nanos ingest_time   (math.MinInt64 = unset)
//	uvarint n, then n × (uvarint len, bytes, uvarint len, bytes)   attrs
//	uvarint len, bytes   traceparent       (len 0 = none)
//
// Decoding is defensive: every length is bounds-checked against the remaining
// input BEFORE allocation, so hostile or corrupt bytes yield an error — never a
// panic or an attacker-controlled allocation. (Fuzz-tested.)
const (
	binMagic    = 0xB1
	unsetTime   = math.MinInt64
	maxBinField = 1 << 16 // largest permitted string field (64 KiB)
	maxBinAttrs = 256
)

var kindByte = map[Kind]byte{KindIoT: 1, KindClick: 2, KindMarket: 3}
var byteKind = [...]Kind{1: KindIoT, 2: KindClick, 3: KindMarket}

// ErrMalformed marks undecodable binary input.
var ErrMalformed = errors.New("malformed binary event")

func putTime(b []byte, t time.Time) []byte {
	v := int64(unsetTime)
	if !t.IsZero() {
		v = t.UnixNano()
	}
	return binary.LittleEndian.AppendUint64(b, uint64(v))
}

func putStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// EncodeBinary serialises e. It fails only for an unknown kind or oversized field.
func EncodeBinary(e Event) ([]byte, error) {
	kb, ok := kindByte[e.Kind]
	if !ok {
		return nil, fmt.Errorf("%w: unknown kind %q", ErrInvalid, e.Kind)
	}
	if len(e.ID) > maxBinField || len(e.Key) > maxBinField || len(e.TraceParent) > maxBinField || len(e.Attrs) > maxBinAttrs {
		return nil, fmt.Errorf("%w: field too large", ErrInvalid)
	}
	b := make([]byte, 0, 96+len(e.ID)+len(e.Key))
	b = append(b, binMagic, kb)
	b = putStr(b, e.ID)
	b = putStr(b, e.Key)
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(e.Value))
	b = putTime(b, e.EventTime)
	b = putTime(b, e.IngestTime)
	b = binary.AppendUvarint(b, uint64(len(e.Attrs)))
	for k, v := range e.Attrs {
		if len(k) > maxBinField || len(v) > maxBinField {
			return nil, fmt.Errorf("%w: attr too large", ErrInvalid)
		}
		b = putStr(putStr(b, k), v)
	}
	return putStr(b, e.TraceParent), nil
}

// reader is a bounds-checked cursor; the first failure sticks, so decode code can
// read straight through and check once.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail(msg string) { // first error wins
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrMalformed, msg)
	}
}

func (r *reader) u64() uint64 {
	if len(r.b) < 8 {
		r.fail("truncated fixed field")
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *reader) uvarint(limit uint64) uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 || v > limit {
		r.fail("bad length")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) str() string {
	n := r.uvarint(maxBinField)
	if r.err != nil {
		return ""
	}
	if uint64(len(r.b)) < n { // check BEFORE slicing/allocating
		r.fail("string overruns input")
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

func (r *reader) time() time.Time {
	v := int64(r.u64())
	if v == unsetTime {
		return time.Time{}
	}
	return time.Unix(0, v).UTC()
}

// DecodeBinary parses the binary format.
func DecodeBinary(b []byte) (Event, error) {
	if len(b) < 2 || b[0] != binMagic {
		return Event{}, fmt.Errorf("%w: bad magic", ErrMalformed)
	}
	if int(b[1]) >= len(byteKind) || byteKind[b[1]] == "" {
		return Event{}, fmt.Errorf("%w: unknown kind byte %d", ErrMalformed, b[1])
	}
	r := &reader{b: b[2:]}
	e := Event{Kind: byteKind[b[1]]}
	e.ID = r.str()
	e.Key = r.str()
	e.Value = math.Float64frombits(r.u64())
	e.EventTime = r.time()
	e.IngestTime = r.time()
	if n := r.uvarint(maxBinAttrs); r.err == nil && n > 0 {
		e.Attrs = make(map[string]string, n)
		for i := uint64(0); i < n && r.err == nil; i++ {
			k := r.str()
			e.Attrs[k] = r.str()
		}
	}
	e.TraceParent = r.str()
	if r.err != nil {
		return Event{}, r.err
	}
	if len(r.b) != 0 {
		return Event{}, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(r.b))
	}
	return e, nil
}

// DecodeAny sniffs the first byte and decodes either format.
func DecodeAny(b []byte) (Event, error) {
	if len(b) > 0 && b[0] == binMagic {
		return DecodeBinary(b)
	}
	return Decode(b)
}

// Codec names a wire format for configuration.
type Codec string

const (
	CodecJSON   Codec = "json"
	CodecBinary Codec = "binary"
)

// ParseCodec validates a codec name ("" means json).
func ParseCodec(s string) (Codec, error) {
	switch Codec(s) {
	case "", CodecJSON:
		return CodecJSON, nil
	case CodecBinary:
		return CodecBinary, nil
	}
	return "", fmt.Errorf("unknown codec %q (want json|binary)", s)
}

// EncodeWith serialises e with the chosen codec.
func EncodeWith(c Codec, e Event) ([]byte, error) {
	if c == CodecBinary {
		return EncodeBinary(e)
	}
	return Encode(e)
}
