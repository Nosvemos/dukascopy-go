package csvout

import (
	"time"
)

type marketHolidayKind int

const (
	marketHolidayNone marketHolidayKind = iota
	marketHolidayEarlyClose
	marketHolidayFullClose
)

func isLikelyHolidayMarketClosed(timestamp time.Time, profile string) bool {
	start, end, ok := holidayClosureWindow(timestamp, profile)
	if !ok {
		return false
	}
	return !timestamp.Before(start) && timestamp.Before(end)
}

func nextHolidayClosureBoundary(timestamp time.Time, profile string) (time.Time, bool) {
	_, end, ok := holidayClosureWindow(timestamp, profile)
	if !ok || !end.After(timestamp) {
		return time.Time{}, false
	}
	return end.UTC(), true
}

func holidayClosureWindow(timestamp time.Time, profile string) (time.Time, time.Time, bool) {
	local := timestamp.In(gapMarketLocation())
	dayStart := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, local.Location())
	var kind marketHolidayKind
	if profile == MarketProfileEquity {
		kind = nyseHolidayKind(dayStart)
	} else {
		kind = usMarketHolidayKind(dayStart)
	}
	if kind == marketHolidayNone {
		return time.Time{}, time.Time{}, false
	}

	at := func(hour int, minute int) time.Time {
		return time.Date(local.Year(), local.Month(), local.Day(), hour, minute, 0, 0, local.Location()).UTC()
	}

	switch kind {
	case marketHolidayFullClose:
		switch profile {
		case MarketProfileOTC24x5:
			return at(0, 0), at(18, 0), true
		case MarketProfileFX24x5:
			return at(0, 0), at(17, 0), true
		case MarketProfileEquity:
			return at(9, 30), at(16, 0), true
		}
	case marketHolidayEarlyClose:
		switch profile {
		case MarketProfileOTC24x5:
			return at(13, 0), at(18, 0), true
		case MarketProfileFX24x5:
			return at(13, 0), at(17, 0), true
		case MarketProfileEquity:
			return at(13, 0), at(16, 0), true
		}
	}

	return time.Time{}, time.Time{}, false
}

// usMarketHolidayKind classifies a New York calendar day for the OTC and FX
// profiles. Good Friday, Christmas Day and New Year's Day (on their actual
// calendar dates, since OTC/FX liquidity disappears on the day itself) are
// full closes; the other US federal market holidays (on their observed
// weekdays) trade a shortened session.
func usMarketHolidayKind(localDay time.Time) marketHolidayKind {
	year, month, day := localDay.Date()
	loc := localDay.Location()
	date := time.Date(year, month, day, 0, 0, 0, 0, loc)

	switch {
	case month == time.January && day == 1,
		month == time.December && day == 25,
		sameLocalDay(date, goodFriday(year, loc)):
		return marketHolidayFullClose
	case sameLocalDay(date, nthWeekdayOfMonth(year, time.January, time.Monday, 3, loc)),
		sameLocalDay(date, nthWeekdayOfMonth(year, time.February, time.Monday, 3, loc)),
		sameLocalDay(date, lastWeekdayOfMonth(year, time.May, time.Monday, loc)),
		year >= 2022 && sameLocalDay(date, observedFixedHoliday(year, time.June, 19, loc)),
		sameLocalDay(date, observedFixedHoliday(year, time.July, 4, loc)),
		sameLocalDay(date, nthWeekdayOfMonth(year, time.September, time.Monday, 1, loc)),
		sameLocalDay(date, nthWeekdayOfMonth(year, time.November, time.Thursday, 4, loc)):
		return marketHolidayEarlyClose
	}
	return marketHolidayNone
}

// nyseHolidayKind classifies a New York calendar day following NYSE's
// published rules:
//
//   - Full closes: New Year's Day, Martin Luther King Jr. Day, Washington's
//     Birthday, Good Friday, Memorial Day, Juneteenth (from 2022),
//     Independence Day, Labor Day, Thanksgiving Day and Christmas Day.
//     A holiday on Saturday is observed the preceding Friday and one on
//     Sunday the following Monday, except that New Year's Day on a Saturday
//     is not observed (the preceding Friday is a normal session).
//   - Early closes (13:00): July 3 when it is Monday-Thursday, the day after
//     Thanksgiving, and December 24 when it is Monday-Thursday.
//
// One-off closures (national days of mourning, weather) are not modelled.
func nyseHolidayKind(localDay time.Time) marketHolidayKind {
	year, month, day := localDay.Date()
	loc := localDay.Location()
	date := time.Date(year, month, day, 0, 0, 0, 0, loc)
	weekday := date.Weekday()
	if weekday == time.Saturday || weekday == time.Sunday {
		return marketHolidayNone
	}

	fullCloses := []time.Time{
		nyseNewYearObserved(year, loc),
		nthWeekdayOfMonth(year, time.January, time.Monday, 3, loc),
		nthWeekdayOfMonth(year, time.February, time.Monday, 3, loc),
		goodFriday(year, loc),
		lastWeekdayOfMonth(year, time.May, time.Monday, loc),
		observedFixedHoliday(year, time.July, 4, loc),
		nthWeekdayOfMonth(year, time.September, time.Monday, 1, loc),
		nthWeekdayOfMonth(year, time.November, time.Thursday, 4, loc),
		observedFixedHoliday(year, time.December, 25, loc),
	}
	if year >= 2022 {
		fullCloses = append(fullCloses, observedFixedHoliday(year, time.June, 19, loc))
	}
	for _, holiday := range fullCloses {
		if sameLocalDay(date, holiday) {
			return marketHolidayFullClose
		}
	}

	switch {
	case month == time.July && day == 3 && weekday != time.Friday,
		month == time.December && day == 24 && weekday != time.Friday,
		sameLocalDay(date, nthWeekdayOfMonth(year, time.November, time.Thursday, 4, loc).AddDate(0, 0, 1)):
		return marketHolidayEarlyClose
	}
	return marketHolidayNone
}

// nyseNewYearObserved returns the NYSE New Year's Day closure for year: Jan 1,
// or Monday Jan 2 when Jan 1 is a Sunday. When Jan 1 is a Saturday there is no
// closure, signalled by returning the Saturday itself (never a trading day).
func nyseNewYearObserved(year int, location *time.Location) time.Time {
	date := time.Date(year, time.January, 1, 0, 0, 0, 0, location)
	if date.Weekday() == time.Sunday {
		return date.AddDate(0, 0, 1)
	}
	return date
}

func sameLocalDay(left time.Time, right time.Time) bool {
	ly, lm, ld := left.Date()
	ry, rm, rd := right.Date()
	return ly == ry && lm == rm && ld == rd
}

func observedFixedHoliday(year int, month time.Month, day int, location *time.Location) time.Time {
	date := time.Date(year, month, day, 0, 0, 0, 0, location)
	switch date.Weekday() {
	case time.Saturday:
		return date.AddDate(0, 0, -1)
	case time.Sunday:
		return date.AddDate(0, 0, 1)
	default:
		return date
	}
}

func nthWeekdayOfMonth(year int, month time.Month, weekday time.Weekday, n int, location *time.Location) time.Time {
	date := time.Date(year, month, 1, 0, 0, 0, 0, location)
	for date.Weekday() != weekday {
		date = date.AddDate(0, 0, 1)
	}
	return date.AddDate(0, 0, (n-1)*7)
}

func lastWeekdayOfMonth(year int, month time.Month, weekday time.Weekday, location *time.Location) time.Time {
	date := time.Date(year, month+1, 0, 0, 0, 0, 0, location)
	for date.Weekday() != weekday {
		date = date.AddDate(0, 0, -1)
	}
	return date
}

func goodFriday(year int, location *time.Location) time.Time {
	return easterSunday(year, location).AddDate(0, 0, -2)
}

func easterSunday(year int, location *time.Location) time.Time {
	a := year % 19
	b := year / 100
	c := year % 100
	d := b / 4
	e := b % 4
	f := (b + 8) / 25
	g := (b - f + 1) / 3
	h := (19*a + b - d - g + 15) % 30
	i := c / 4
	k := c % 4
	l := (32 + 2*e + 2*i - h - k) % 7
	m := (a + 11*h + 22*l) / 451
	month := (h + l - 7*m + 114) / 31
	day := ((h + l - 7*m + 114) % 31) + 1
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, location)
}
