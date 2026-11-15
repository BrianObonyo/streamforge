package events

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func full() Event {
	return Event{ID: "abc123", Kind: KindMarket, Key: "BTC-USD", Value: 64123.5,
		EventTime: now, IngestTime: now.Add(5 * time.Millisecond),
		Attrs:       map[string]string{"currency": "USD", "ünï": "çödé"},
		TraceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
}

func TestBinaryRoundTrip(t *testing.T) {
	for _, in := range []Event{full(), {ID: "x", Kind: KindIoT, Key: "k"}} { // second: zero times, nil attrs
		b, err := EncodeBinary(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := DecodeBinary(b)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !out.EventTime.Equal(in.EventTime) || !out.IngestTime.Equal(in.IngestTime) {
			t.Fatalf("times: %v/%v vs %v/%v", out.EventTime, out.IngestTime, in.EventTime, in.IngestTime)
		}
		out.EventTime, out.IngestTime, in.EventTime, in.IngestTime = time.Time{}, time.Time{}, time.Time{}, time.Time{}
		if !reflect.DeepEqual(in, out) && !(len(in.Attrs) == 0 && len(out.Attrs) == 0 && in.ID == out.ID) {
			t.Fatalf("round trip:\n in=%+v\nout=%+v", in, out)
		}
	}
}

func TestBinaryPreservesSpecialFloats(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(-1), math.Copysign(0, -1), math.SmallestNonzeroFloat64} {
		e := full()
		e.Value = v
		b, _ := EncodeBinary(e)
		out, err := DecodeBinary(b)
		if err != nil || math.Float64bits(out.Value) != math.Float64bits(v) {
			t.Fatalf("value %v not preserved: %v err=%v", v, out.Value, err)
		}
	}
	// NaN must still be REJECTED by validation even though the codec carries it.
	e := full()
	e.Value = math.NaN()
	if e.Validate(now) == nil {
		t.Fatal("NaN passed validation")
	}
}

func TestDecodeAnySniffsFormat(t *testing.T) {
	j, _ := Encode(full())
	b, _ := EncodeBinary(full())
	for name, raw := range map[string][]byte{"json": j, "binary": b} {
		got, err := DecodeAny(raw)
		if err != nil || got.ID != "abc123" || got.TraceParent == "" {
			t.Fatalf("%s: %+v err=%v", name, got, err)
		}
	}
}

func TestBinaryRejectsMalformed(t *testing.T) {
	good, _ := EncodeBinary(full())
	cases := map[string][]byte{
		"empty":        {},
		"magic only":   {binMagic},
		"bad magic":    append([]byte{0x00}, good[1:]...),
		"bad kind":     append([]byte{binMagic, 9}, good[2:]...),
		"truncated":    good[:len(good)/2],
		"trailing":     append(append([]byte{}, good...), 0xFF),
		"huge length":  {binMagic, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F},
		"overlong var": {binMagic, 1, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01},
	}
	for name, b := range cases {
		if _, err := DecodeBinary(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := EncodeBinary(Event{Kind: "nope"}); err == nil {
		t.Error("unknown kind encoded")
	}
}

// Every prefix of a valid message must fail cleanly (no panic, no success).
func TestBinaryEveryTruncationFails(t *testing.T) {
	good, _ := EncodeBinary(full())
	for i := 0; i < len(good); i++ {
		if _, err := DecodeBinary(good[:i]); err == nil {
			t.Fatalf("prefix of %d/%d bytes decoded successfully", i, len(good))
		}
	}
}

func FuzzDecodeBinary(f *testing.F) {
	good, _ := EncodeBinary(full())
	f.Add(good)
	f.Add([]byte{binMagic, 1})
	f.Fuzz(func(t *testing.T, b []byte) {
		e, err := DecodeBinary(b) // must never panic
		if err == nil {
			if again, err2 := EncodeBinary(e); err2 == nil { // and valid output must re-encode & re-decode
				if _, err3 := DecodeBinary(again); err3 != nil {
					t.Fatalf("re-decode failed: %v", err3)
				}
			}
		}
	})
}

func TestParseCodec(t *testing.T) {
	if c, err := ParseCodec(""); err != nil || c != CodecJSON {
		t.Fatal(c, err)
	}
	if _, err := ParseCodec("xml"); err == nil {
		t.Fatal("xml accepted")
	}
}
