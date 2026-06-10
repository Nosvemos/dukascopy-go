package cli

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func prepareResume(enabled bool, outputPath string, resultKind dukascopy.ResultKind, barColumns []string, tickColumns []string, request *dukascopy.DownloadRequest) (*csvout.ResumeState, []string, error) {
	if !enabled {
		return nil, nil, nil
	}

	expectedColumns := barColumns
	if resultKind == dukascopy.ResultKindTick {
		expectedColumns = tickColumns
	}
	if !csvout.ColumnsContainTimestamp(expectedColumns) {
		return nil, nil, errors.New("--resume requires the selected columns to include timestamp")
	}

	state, err := csvout.InspectExistingCSV(outputPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	if len(state.Columns) > 0 && !csvout.HeadersMatch(expectedColumns, state.Columns) {
		return nil, nil, fmt.Errorf("existing CSV header does not match the selected columns for %s", outputPath)
	}

	var dedupeRecord []string
	if state.HasRows && (request.From.Before(state.LastTime) || request.From.Equal(state.LastTime)) {
		inclusiveTo := request.To.Add(-time.Nanosecond)
		if !state.LastTime.Before(inclusiveTo) {
			request.From = request.To
			return &state, nil, nil
		}
		request.From = state.LastTime
		dedupeRecord = state.LastRecord
	}

	return &state, dedupeRecord, nil
}

func writeTickOutput(outputPath string, resumeState *csvout.ResumeState, dedupeRecord []string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) (int, error) {
	if resumeState == nil || !resumeState.Exists || len(resumeState.Columns) == 0 {
		if err := csvout.WriteTicks(outputPath, instrument, columns, ticks); err != nil {
			return 0, err
		}
		return len(ticks), nil
	}

	tempPath, err := createResumeTempPath(outputPath)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tempPath)

	if err := csvout.WriteTicks(tempPath, instrument, columns, ticks); err != nil {
		return 0, err
	}

	return csvout.MergeResumeCSV(outputPath, tempPath, dedupeRecord)
}

func writeBarOutput(outputPath string, resumeState *csvout.ResumeState, dedupeRecord []string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) (int, error) {
	if resumeState == nil || !resumeState.Exists || len(resumeState.Columns) == 0 {
		if err := csvout.WriteBars(outputPath, instrument, columns, primaryBars, bidBars, askBars); err != nil {
			return 0, err
		}
		if csvout.BarColumnsNeedBidAsk(columns) {
			return len(bidBars), nil
		}
		return len(primaryBars), nil
	}

	tempPath, err := createResumeTempPath(outputPath)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tempPath)

	if err := csvout.WriteBars(tempPath, instrument, columns, primaryBars, bidBars, askBars); err != nil {
		return 0, err
	}

	return csvout.MergeResumeCSV(outputPath, tempPath, dedupeRecord)
}
