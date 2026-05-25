package dukascopy

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"

	"github.com/ulikunitz/xz/lzma"
)

// DecodeTicksBi5 decompresses an LZMA-compressed byte stream from Dukascopy
// and parses standard 20-byte tick records:
//   - Milliseconds since start of the hour (4 bytes, Big-Endian uint32)
//   - Ask price scaled (4 bytes, Big-Endian uint32)
//   - Bid price scaled (4 bytes, Big-Endian uint32)
//   - Ask volume (4 bytes, Big-Endian float32)
//   - Bid volume (4 bytes, Big-Endian float32)
func DecodeTicksBi5(r io.Reader, baseTime time.Time, priceScale int) ([]Tick, error) {
	lzmaReader, err := lzma.NewReader(r)
	if err != nil {
		return nil, err
	}

	var ticks []Tick
	buf := make([]byte, 20)
	factor := math.Pow10(priceScale)

	for {
		_, err := io.ReadFull(lzmaReader, buf)
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		msOffset := binary.BigEndian.Uint32(buf[0:4])
		askRaw := binary.BigEndian.Uint32(buf[4:8])
		bidRaw := binary.BigEndian.Uint32(buf[8:12])
		askVolBits := binary.BigEndian.Uint32(buf[12:16])
		bidVolBits := binary.BigEndian.Uint32(buf[16:20])

		askPrice := float64(askRaw) / factor
		bidPrice := float64(bidRaw) / factor
		askVol := float64(math.Float32frombits(askVolBits))
		bidVol := float64(math.Float32frombits(bidVolBits))

		tickTime := baseTime.Add(time.Duration(msOffset) * time.Millisecond)

		ticks = append(ticks, Tick{
			Time:      tickTime,
			Ask:       askPrice,
			Bid:       bidPrice,
			AskVolume: askVol,
			BidVolume: bidVol,
		})
	}

	return ticks, nil
}

// DecodeBarsBi5 decompresses an LZMA-compressed byte stream from Dukascopy
// and parses standard 24-byte candle (OHLCV) records:
//   - Seconds since start of the day (4 bytes, Big-Endian uint32)
//   - Open price scaled (4 bytes, Big-Endian uint32)
//   - Close price scaled (4 bytes, Big-Endian uint32)
//   - Low price scaled (4 bytes, Big-Endian uint32)
//   - High price scaled (4 bytes, Big-Endian uint32)
//   - Volume (4 bytes, Big-Endian float32)
func DecodeBarsBi5(r io.Reader, baseTime time.Time, priceScale int) ([]Bar, error) {
	lzmaReader, err := lzma.NewReader(r)
	if err != nil {
		return nil, err
	}

	var bars []Bar
	buf := make([]byte, 24)
	factor := math.Pow10(priceScale)

	for {
		_, err := io.ReadFull(lzmaReader, buf)
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		secOffset := binary.BigEndian.Uint32(buf[0:4])
		openRaw := binary.BigEndian.Uint32(buf[4:8])
		closeRaw := binary.BigEndian.Uint32(buf[8:12])
		lowRaw := binary.BigEndian.Uint32(buf[12:16])
		highRaw := binary.BigEndian.Uint32(buf[16:20])
		volBits := binary.BigEndian.Uint32(buf[20:24])

		open := float64(openRaw) / factor
		closePrice := float64(closeRaw) / factor
		low := float64(lowRaw) / factor
		high := float64(highRaw) / factor
		vol := float64(math.Float32frombits(volBits))

		// If high and low are inverted or Close/High/Low are parsed differently,
		// we ensure we normalize them logically.
		// Standard: Seconds, Open, Close, Low, High, Volume
		// To be safe and compliant, we enforce high >= low by checking and swapping if needed
		if low > high {
			low, high = high, low
		}

		barTime := baseTime.Add(time.Duration(secOffset) * time.Second)

		bars = append(bars, Bar{
			Time:   barTime,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  closePrice,
			Volume: vol,
		})
	}

	return bars, nil
}
