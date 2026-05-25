package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

//export FreeString
func FreeString(s *C.char) {
	if s != nil {
		C.free(unsafe.Pointer(s))
	}
}


//export DownloadData
func DownloadData(
	symbol *C.char,
	timeframe *C.char,
	side *C.char,
	fromDate *C.char,
	toDate *C.char,
	outputPath *C.char,
	engine *C.char,
	priceScale C.int,
) *C.char {
	sym := C.GoString(symbol)
	tf := C.GoString(timeframe)
	sd := C.GoString(side)
	fromStr := C.GoString(fromDate)
	toStr := C.GoString(toDate)
	out := C.GoString(outputPath)
	eng := C.GoString(engine)

	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		return C.CString(fmt.Sprintf("invalid from date: %v", err))
	}
	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		return C.CString(fmt.Sprintf("invalid to date: %v", err))
	}

	client := dukascopy.NewClient("https://jetta.dukascopy.com", 30*time.Second).
		WithEngine(dukascopy.Engine(strings.ToLower(strings.TrimSpace(eng)))).
		WithRetries(3).
		WithBackoff(500 * time.Millisecond).
		WithRateLimit(0)

	normalizedTimeframe := dukascopy.NormalizeGranularity(dukascopy.Granularity(tf))
	request := dukascopy.DownloadRequest{
		Symbol:      sym,
		Granularity: normalizedTimeframe,
		Side:        dukascopy.PriceSide(sd),
		From:        from.UTC(),
		To:          to.UTC(),
	}

	ctx := context.Background()
	res, err := client.Download(ctx, request)
	if err != nil {
		return C.CString(fmt.Sprintf("download failed: %v", err))
	}

	profile := csvout.ProfileSimple
	if res.Kind == dukascopy.ResultKindBar {
		barColumns := csvout.BarColumnsForProfile(profile)
		err = csvout.WriteBars(out, dukascopy.Instrument{PriceScale: int(priceScale)}, barColumns, res.Bars, nil, nil)
	} else {
		tickColumns := csvout.TickColumnsForProfile(profile)
		err = csvout.WriteTicks(out, dukascopy.Instrument{PriceScale: int(priceScale)}, tickColumns, res.Ticks)
	}

	if err != nil {
		return C.CString(fmt.Sprintf("export failed: %v", err))
	}

	return nil
}

func main() {}

