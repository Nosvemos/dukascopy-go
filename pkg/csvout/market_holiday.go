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
	kind := usMarketHolidayKind(dayStart)
	if kind == marketHolidayNone {
		return time.Time{}, time.Time{}, false
	}

	switch kind {
	case marketHolidayFullClose:
		switch profile {
		case MarketProfileOTC24x5:
			start := dayStart
			end := dayStart.Add(18 * time.Hour)
			return start.UTC(), end.UTC(), true
		case MarketProfileFX24x5:
			start := dayStart
			end := dayStart.Add(17 * time.Hour)
			return start.UTC(), end.UTC(), true
		}
	case marketHolidayEarlyClose:
		switch profile {
		case MarketProfileOTC24x5:
			start := time.Date(local.Year(), local.Month(), local.Day(), 13, 0, 0, 0, local.Location())
			end := time.Date(local.Year(), local.Month(), local.Day(), 18, 0, 0, 0, local.Location())
			return start.UTC(), end.UTC(), true
		case MarketProfileFX24x5:
			start := time.Date(local.Year(), local.Month(), local.Day(), 13, 0, 0, 0, local.Location())
			end := time.Date(local.Year(), local.Month(), local.Day(), 17, 0, 0, 0, local.Location())
			return start.UTC(), end.UTC(), true
		}
	}

	return time.Time{}, time.Time{}, false
}

func usMarketHolidayKind(localDay time.Time) marketHolidayKind {
	year, month, day := localDay.Date()
	date := time.Date(year, month, day, 0, 0, 0, 0, localDay.Location())

	if sameLocalDay(date, nthWeekdayOfMonth(year, time.January, time.Monday, 3, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, nthWeekdayOfMonth(year, time.February, time.Monday, 3, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, goodFriday(year, localDay.Location())) {
		return marketHolidayFullClose
	}
	if sameLocalDay(date, lastWeekdayOfMonth(year, time.May, time.Monday, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if year >= 2022 && sameLocalDay(date, observedFixedHoliday(year, time.June, 19, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, observedFixedHoliday(year, time.July, 4, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, nthWeekdayOfMonth(year, time.September, time.Monday, 1, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, nthWeekdayOfMonth(year, time.November, time.Thursday, 4, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, observedFixedHoliday(year, time.December, 25, localDay.Location())) {
		return marketHolidayFullClose
	}
	if sameLocalDay(date, observedFixedHoliday(year, time.January, 1, localDay.Location())) {
		return marketHolidayFullClose
	}
	return marketHolidayNone
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
