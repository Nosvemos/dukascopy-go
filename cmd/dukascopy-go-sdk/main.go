package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
	"os"
	"time"
	"unsafe"

	"github.com/Nosvemos/dukascopy-go/internal/cli"
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
	profile *C.char,
	timezone *C.char,
	customColumns *C.char,
	resume C.int,
	parallelism C.int,
	partition *C.char,
	fillGaps *C.char,
	hive C.int,
) *C.char {
	sym := C.GoString(symbol)
	tf := C.GoString(timeframe)
	sd := C.GoString(side)
	fromStr := C.GoString(fromDate)
	toStr := C.GoString(toDate)
	out := C.GoString(outputPath)
	eng := C.GoString(engine)
	prof := C.GoString(profile)
	tz := C.GoString(timezone)
	cc := C.GoString(customColumns)
	resVal := resume != 0
	parVal := int(parallelism)
	partVal := C.GoString(partition)
	fg := C.GoString(fillGaps)
	hiveVal := hive != 0

	from, err := cli.ParseFlexibleTime(fromStr)
	if err != nil {
		return C.CString(fmt.Sprintf("invalid from date: %v", err))
	}
	to, err := cli.ParseFlexibleTime(toStr)
	if err != nil {
		return C.CString(fmt.Sprintf("invalid to date: %v", err))
	}

	opts := cli.SDKDownloadOptions{
		Symbol:        sym,
		Timeframe:     tf,
		Side:          sd,
		From:          from,
		To:            to,
		OutputPath:    out,
		Engine:        eng,
		PriceScale:    int(priceScale),
		Profile:       prof,
		Timezone:      tz,
		CustomColumns: cc,
		Resume:        resVal,
		Parallelism:   parVal,
		Partition:     partVal,
		FillGaps:      fg,
		Hive:          hiveVal,
	}

	ctx := context.Background()
	if err := cli.RunSDKDownload(ctx, opts); err != nil {
		return C.CString(fmt.Sprintf("download failed: %v", err))
	}

	return nil
}

//export DBLoadData
func DBLoadData(
	dbType *C.char,
	dbURL *C.char,
	tableName *C.char,
	inputPath *C.char,
	user *C.char,
	password *C.char,
	token *C.char,
	org *C.char,
	bucket *C.char,
	symbolTag *C.char,
	batchSize C.int,
	timeoutSec C.int,
) *C.char {
	dbT := C.GoString(dbType)
	url := C.GoString(dbURL)
	tbl := C.GoString(tableName)
	inp := C.GoString(inputPath)
	usr := C.GoString(user)
	pwd := C.GoString(password)
	tok := C.GoString(token)
	o := C.GoString(org)
	b := C.GoString(bucket)
	symTag := C.GoString(symbolTag)

	opts := cli.DBLoadOptions{
		DBType:    dbT,
		DBURL:     url,
		Table:     tbl,
		InputPath: inp,
		User:      usr,
		Password:  pwd,
		Token:     tok,
		Org:       o,
		Bucket:    b,
		SymbolTag: symTag,
		BatchSize: int(batchSize),
		Timeout:   time.Duration(timeoutSec) * time.Second,
	}

	ctx := context.Background()
	err := cli.DBLoad(ctx, os.Stdout, os.Stderr, opts)
	if err != nil {
		return C.CString(fmt.Sprintf("db load failed: %v", err))
	}
	return nil
}

func main() {}
