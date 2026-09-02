package reconciliation

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePSPStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		csv     string
		want    []PSPRecord
		wantErr error
	}{
		{
			name: "should parse every row when the statement is well-formed",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				"REF-1,1000,INR,SETTLED,2026-01-15T12:00:00Z\n" +
				"REF-2,500,USD,FAILED,2026-01-15T13:30:00Z\n",
			want: []PSPRecord{
				{ExternalRef: "REF-1", AmountMinor: 1000, Currency: "INR", Status: "SETTLED",
					SettledAt: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)},
				{ExternalRef: "REF-2", AmountMinor: 500, Currency: "USD", Status: "FAILED",
					SettledAt: time.Date(2026, 1, 15, 13, 30, 0, 0, time.UTC)},
			},
		},
		{
			name: "should accept columns in any order, matched by header name",
			csv: "status,external_ref,settled_at,currency,amount_minor\n" +
				"SETTLED,REF-1,2026-01-15T12:00:00Z,INR,1000\n",
			want: []PSPRecord{
				{ExternalRef: "REF-1", AmountMinor: 1000, Currency: "INR", Status: "SETTLED",
					SettledAt: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)},
			},
		},
		{
			name: "should lowercase a status written in mixed case",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				"REF-1,1000,INR,settled,2026-01-15T12:00:00Z\n",
			want: []PSPRecord{
				{ExternalRef: "REF-1", AmountMinor: 1000, Currency: "INR", Status: "SETTLED",
					SettledAt: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)},
			},
		},
		{
			name:    "should reject a statement with no data rows",
			csv:     "external_ref,amount_minor,currency,status,settled_at\n",
			wantErr: ErrEmptyStatement,
		},
		{
			name:    "should reject a completely empty file",
			csv:     "",
			wantErr: ErrEmptyStatement,
		},
		{
			name:    "should reject a header missing a required column",
			csv:     "external_ref,amount_minor,currency,status\nREF-1,1000,INR,SETTLED\n",
			wantErr: ErrMalformedRecord,
		},
		{
			name: "should reject a row with an empty external_ref",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				",1000,INR,SETTLED,2026-01-15T12:00:00Z\n",
			wantErr: ErrMalformedRecord,
		},
		{
			name: "should reject a non-integer amount",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				"REF-1,not-a-number,INR,SETTLED,2026-01-15T12:00:00Z\n",
			wantErr: ErrMalformedRecord,
		},
		{
			name: "should reject a zero amount",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				"REF-1,0,INR,SETTLED,2026-01-15T12:00:00Z\n",
			wantErr: ErrMalformedRecord,
		},
		{
			name: "should reject a currency that is not three letters",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				"REF-1,1000,INRX,SETTLED,2026-01-15T12:00:00Z\n",
			wantErr: ErrMalformedRecord,
		},
		{
			name: "should reject a status this package does not recognise",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				"REF-1,1000,INR,PROCESSING,2026-01-15T12:00:00Z\n",
			wantErr: ErrMalformedRecord,
		},
		{
			name: "should reject a settled_at that is not RFC3339",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				"REF-1,1000,INR,SETTLED,15 Jan 2026\n",
			wantErr: ErrMalformedRecord,
		},
		{
			name: "should reject the whole file when one row in the middle is malformed",
			csv: "external_ref,amount_minor,currency,status,settled_at\n" +
				"REF-1,1000,INR,SETTLED,2026-01-15T12:00:00Z\n" +
				"REF-2,not-a-number,INR,SETTLED,2026-01-15T12:00:00Z\n" +
				"REF-3,1000,INR,SETTLED,2026-01-15T12:00:00Z\n",
			wantErr: ErrMalformedRecord,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParsePSPStatement(strings.NewReader(tt.csv))
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.wantErr), "got %v, want an error wrapping %v", err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
