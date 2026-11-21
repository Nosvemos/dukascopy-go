package cli

import "testing"

func TestBuildCopyInStatement(t *testing.T) {
	cases := []struct {
		table   string
		columns []string
		want    string
	}{
		{"bars", []string{"timestamp", "open"}, `COPY "bars" ("timestamp", "open") FROM STDIN`},
		{"market.bars", []string{"close"}, `COPY "market"."bars" ("close") FROM STDIN`},
		{`"market"."bars"`, nil, `COPY "market"."bars" FROM STDIN`},
		{`we"ird`, []string{`c"ol`}, `COPY "we""ird" ("c""ol") FROM STDIN`},
	}
	for _, tc := range cases {
		if got := buildCopyInStatement(tc.table, tc.columns); got != tc.want {
			t.Errorf("buildCopyInStatement(%q, %q) = %q, want %q", tc.table, tc.columns, got, tc.want)
		}
	}
}
