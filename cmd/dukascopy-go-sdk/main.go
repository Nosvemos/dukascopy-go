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
	priceScale C.int, // ignored: the instrument catalog supplies the price scale (kept for ABI compatibility)
	profile *C.char,
	timezone *C.char,
	customColumns *C.char,
	resume C.int,
	parallelism C.int,
	partition *C.char,
	fillGaps *C.char,
	hive C.int,
) (result *C.char) {
	// A Go panic must never unwind into (and kill) the host process.
	defer recoverToCString(&result)

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

	return errorCString(callGuarded(func() error {
		from, err := cli.ParseFlexibleTime(fromStr)
		if err != nil {
			return fmt.Errorf("invalid from date: %v", err)
		}
		to, err := cli.ParseFlexibleTime(toStr)
		if err != nil {
			return fmt.Errorf("invalid to date: %v", err)
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

		if err := cli.RunSDKDownload(context.Background(), opts); err != nil {
			return fmt.Errorf("download failed: %v", err)
		}
		return nil
	}))
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
) (result *C.char) {
	// A Go panic must never unwind into (and kill) the host process.
	defer recoverToCString(&result)

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

	return errorCString(callGuarded(func() error {
		if err := cli.DBLoad(context.Background(), os.Stdout, os.Stderr, opts); err != nil {
			return fmt.Errorf("db load failed: %v", err)
		}
		return nil
	}))
}

// recoverToCString turns a panic in an exported function into an error
// string returned to the caller (who frees it with FreeString).
func recoverToCString(result **C.char) {
	if recovered := recover(); recovered != nil {
		*result = C.CString(panicMessage(recovered))
	}
}

// callGuarded runs fn and returns its error text ("" on success). A panic in
// fn is recovered and returned as an error text instead of crashing the host
// process (the Python interpreter that loaded the library).
func callGuarded(fn func() error) (message string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			message = panicMessage(recovered)
		}
	}()
	if err := fn(); err != nil {
		return err.Error()
	}
	return ""
}

// errorCString converts an error text to a C string; "" becomes NULL
// (success).
func errorCString(message string) *C.char {
	if message == "" {
		return nil
	}
	return C.CString(message)
}

// panicMessage formats a recovered panic value as the error returned to the
// SDK caller.
func panicMessage(recovered any) string {
	return fmt.Sprintf("internal error: %v", recovered)
}

func main() {}
