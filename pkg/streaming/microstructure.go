package streaming

import (
	"math"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// MicrostructureMetrics holds simple per-tick quote metrics.
//
// Several field names are borrowed from the market-microstructure literature
// but, because Dukascopy ticks are top-of-book quotes without trade prints or
// trade direction, they are NOT the textbook measures of the same name. The
// names are kept for backward compatibility; each field documents exactly
// what is computed. "volume" below is the quoted top-of-book liquidity
// (BidVolume / AskVolume), not traded volume.
type MicrostructureMetrics struct {
	// Spread is the quoted spread: Ask - Bid of the current tick.
	Spread float64 `json:"spread"`
	// MidPrice is (Ask + Bid) / 2 of the current tick.
	MidPrice float64 `json:"mid_price"`
	// OrderFlowImbalance is the top-of-book depth imbalance of the current
	// tick, (BidVolume - AskVolume) / (BidVolume + AskVolume), in [-1, +1]
	// (0 when both volumes are zero). It is a static queue-size imbalance,
	// not Cont/Kukanov/Stoikov order-flow imbalance, which is built from
	// changes in the best quotes and sizes between consecutive ticks.
	OrderFlowImbalance float64 `json:"order_flow_imbalance"`
	// EffectiveSpread is 2 * |mid(current) - mid(previous)|, i.e. twice the
	// absolute mid-price change between two consecutive quotes (0 without a
	// previous tick). The literature's effective spread is
	// 2 * |trade price - mid| for an executed trade, which cannot be derived
	// from quote ticks.
	EffectiveSpread float64 `json:"effective_spread"`
	// AmihudIlliquidity is |mid(current) - mid(previous)| divided by the
	// current tick's BidVolume + AskVolume (0 without a previous tick or
	// with zero volume). Amihud (2002) is the average of |return| / traded
	// dollar volume over a period (usually daily); this is a per-tick
	// absolute price change per unit of quoted depth, not a return over
	// traded value.
	AmihudIlliquidity float64 `json:"amihud_illiquidity"`
	// PriceVelocity is (mid(current) - mid(previous)) divided by the elapsed
	// time in seconds, in price units per second (0 without a previous tick
	// or when the elapsed time is not positive).
	PriceVelocity float64 `json:"price_velocity"`
}

// ComputeMicrostructure computes metrics for a single tick given the previous
// tick (which may be nil). See MicrostructureMetrics for the exact formulas.
func ComputeMicrostructure(current dukascopy.Tick, previous *dukascopy.Tick) MicrostructureMetrics {
	spread := current.Ask - current.Bid
	mid := (current.Ask + current.Bid) / 2.0

	var ofi float64
	totalVol := current.BidVolume + current.AskVolume
	if totalVol > 0 {
		ofi = (current.BidVolume - current.AskVolume) / totalVol
	}

	var effSpread float64
	var amihud float64
	var velocity float64

	if previous != nil {
		prevMid := (previous.Ask + previous.Bid) / 2.0
		diff := mid - prevMid
		effSpread = 2.0 * math.Abs(diff)

		if totalVol > 0 {
			amihud = math.Abs(diff) / totalVol
		}

		dt := current.Time.Sub(previous.Time).Seconds()
		if dt > 0 {
			velocity = diff / dt
		}
	}

	return MicrostructureMetrics{
		Spread:             spread,
		MidPrice:           mid,
		OrderFlowImbalance: ofi,
		EffectiveSpread:    effSpread,
		AmihudIlliquidity:  amihud,
		PriceVelocity:      velocity,
	}
}
