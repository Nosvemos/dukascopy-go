package dukascopy

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/ulikunitz/xz/lzma"
)

func TestDecodeTicksBi5(t *testing.T) {
	var rawData bytes.Buffer

	// Record 1
	// time offset: 100ms
	// ask: 100050 (scaled price for 1.00050)
	// bid: 100000 (scaled price for 1.00000)
	// ask volume: 1.5
	// bid volume: 1.0
	binary.Write(&rawData, binary.BigEndian, uint32(100))
	binary.Write(&rawData, binary.BigEndian, uint32(100050))
	binary.Write(&rawData, binary.BigEndian, uint32(100000))
	binary.Write(&rawData, binary.BigEndian, math.Float32bits(1.5))
	binary.Write(&rawData, binary.BigEndian, math.Float32bits(1.0))

	// Record 2
	// time offset: 500ms
	// ask: 100060 (1.00060)
	// bid: 100010 (1.00010)
	// ask volume: 2.0
	// bid volume: 2.5
	binary.Write(&rawData, binary.BigEndian, uint32(500))
	binary.Write(&rawData, binary.BigEndian, uint32(100060))
	binary.Write(&rawData, binary.BigEndian, uint32(100010))
	binary.Write(&rawData, binary.BigEndian, math.Float32bits(2.0))
	binary.Write(&rawData, binary.BigEndian, math.Float32bits(2.5))

	// Compress rawData using lzma
	var compressedData bytes.Buffer
	w, err := lzma.NewWriter(&compressedData)
	if err != nil {
		t.Fatalf("lzma.NewWriter failed: %v", err)
	}
	if _, err := w.Write(rawData.Bytes()); err != nil {
		t.Fatalf("lzma writer Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("lzma writer Close failed: %v", err)
	}

	baseTime := time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)
	ticks, err := DecodeTicksBi5(&compressedData, baseTime, 5)
	if err != nil {
		t.Fatalf("DecodeTicksBi5 failed: %v", err)
	}

	if len(ticks) != 2 {
		t.Fatalf("expected 2 ticks, got %d", len(ticks))
	}

	// Verify Record 1
	t1 := ticks[0]
	if !t1.Time.Equal(baseTime.Add(100 * time.Millisecond)) {
		t.Errorf("expected t1 time offset 100ms, got %v", t1.Time)
	}
	if t1.Ask != 1.00050 || t1.Bid != 1.00000 {
		t.Errorf("unexpected t1 prices: ask=%f, bid=%f", t1.Ask, t1.Bid)
	}
	if t1.AskVolume != 1.5 || t1.BidVolume != 1.0 {
		t.Errorf("unexpected t1 volumes: ask=%f, bid=%f", t1.AskVolume, t1.BidVolume)
	}

	// Verify Record 2
	t2 := ticks[1]
	if !t2.Time.Equal(baseTime.Add(500 * time.Millisecond)) {
		t.Errorf("expected t2 time offset 500ms, got %v", t2.Time)
	}
	if t2.Ask != 1.00060 || t2.Bid != 1.00010 {
		t.Errorf("unexpected t2 prices: ask=%f, bid=%f", t2.Ask, t2.Bid)
	}
	if t2.AskVolume != 2.0 || t2.BidVolume != 2.5 {
		t.Errorf("unexpected t2 volumes: ask=%f, bid=%f", t2.AskVolume, t2.BidVolume)
	}
}

func TestDecodeBarsBi5(t *testing.T) {
	var rawData bytes.Buffer

	// Record 1
	// time offset: 60s
	// open: 100000 (1.00000)
	// close: 100050 (1.00050)
	// low: 99990 (0.99990)
	// high: 100080 (1.00080)
	// volume: 100.5
	binary.Write(&rawData, binary.BigEndian, uint32(60))
	binary.Write(&rawData, binary.BigEndian, uint32(100000))
	binary.Write(&rawData, binary.BigEndian, uint32(100050))
	binary.Write(&rawData, binary.BigEndian, uint32(99990))
	binary.Write(&rawData, binary.BigEndian, uint32(100080))
	binary.Write(&rawData, binary.BigEndian, math.Float32bits(100.5))

	// Compress rawData using lzma
	var compressedData bytes.Buffer
	w, err := lzma.NewWriter(&compressedData)
	if err != nil {
		t.Fatalf("lzma.NewWriter failed: %v", err)
	}
	if _, err := w.Write(rawData.Bytes()); err != nil {
		t.Fatalf("lzma writer Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("lzma writer Close failed: %v", err)
	}

	baseTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	bars, err := DecodeBarsBi5(&compressedData, baseTime, 5)
	if err != nil {
		t.Fatalf("DecodeBarsBi5 failed: %v", err)
	}

	if len(bars) != 1 {
		t.Fatalf("expected 1 bar, got %d", len(bars))
	}

	b1 := bars[0]
	if !b1.Time.Equal(baseTime.Add(60 * time.Second)) {
		t.Errorf("expected b1 time offset 60s, got %v", b1.Time)
	}
	if b1.Open != 1.00000 || b1.Close != 1.00050 || b1.Low != 0.99990 || b1.High != 1.00080 {
		t.Errorf("unexpected b1 prices: open=%f, close=%f, low=%f, high=%f", b1.Open, b1.Close, b1.Low, b1.High)
	}
	if b1.Volume != 100.5 {
		t.Errorf("unexpected b1 volume: %f", b1.Volume)
	}
}
