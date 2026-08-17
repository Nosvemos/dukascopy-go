package streaming

import (
	"math"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// MicrostructureMetrics holds market microstructure and order flow metrics for HFT / Quants.
type MicrostructureMetrics struct {
	Spread             float64 `json:"spread"`
	MidPrice           float64 `json:"mid_price"`
	OrderFlowImbalance float64 `json:"order_flow_imbalance"` // [-1.0, +1.0]
	EffectiveSpread    float64 `json:"effective_spread"`
	AmihudIlliquidity  float64 `json:"amihud_illiquidity"`
	PriceVelocity      float64 `json:"price_velocity"` // points per second
}

// ComputeMicrostructure computes metrics for a single tick given the previous tick.
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
