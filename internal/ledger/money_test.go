package ledger

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMoney(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		amount   int64
		currency string
		wantErr  error
	}{
		{name: "should build the amount when the currency is a valid ISO code", amount: 1250, currency: "INR"},
		{name: "should build the amount when it is zero", amount: 0, currency: "USD"},
		{name: "should build the amount when it is negative", amount: -500, currency: "USD"},
		{name: "should build the amount when it is the largest int64", amount: math.MaxInt64, currency: "JPY"},
		{name: "should reject the amount when the currency is lowercase", amount: 1, currency: "inr", wantErr: ErrInvalidCurrency},
		{name: "should reject the amount when the currency is too short", amount: 1, currency: "IN", wantErr: ErrInvalidCurrency},
		{name: "should reject the amount when the currency is too long", amount: 1, currency: "INRR", wantErr: ErrInvalidCurrency},
		{name: "should reject the amount when the currency is empty", amount: 1, currency: "", wantErr: ErrInvalidCurrency},
		{name: "should reject the amount when the currency is numeric", amount: 1, currency: "356", wantErr: ErrInvalidCurrency},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, err := NewMoney(tc.amount, tc.currency)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.amount, m.AmountMinor())
			assert.Equal(t, tc.currency, m.Currency())
		})
	}
}

func TestMoneyAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		a, b    Money
		want    int64
		wantErr error
	}{
		{
			name: "should sum the amounts when both share a currency",
			a:    MustNewMoney(1000, "INR"), b: MustNewMoney(250, "INR"), want: 1250,
		},
		{
			name: "should sum the amounts when one is negative",
			a:    MustNewMoney(1000, "INR"), b: MustNewMoney(-250, "INR"), want: 750,
		},
		{
			name: "should sum the amounts when both are negative",
			a:    MustNewMoney(-1000, "INR"), b: MustNewMoney(-250, "INR"), want: -1250,
		},
		{
			name: "should sum the amounts when the result is exactly the int64 maximum",
			a:    MustNewMoney(math.MaxInt64-1, "INR"), b: MustNewMoney(1, "INR"), want: math.MaxInt64,
		},
		{
			name: "should sum the amounts when the result is exactly the int64 minimum",
			a:    MustNewMoney(math.MinInt64+1, "INR"), b: MustNewMoney(-1, "INR"), want: math.MinInt64,
		},
		{
			name: "should reject the sum when it overflows past the int64 maximum",
			a:    MustNewMoney(math.MaxInt64, "INR"), b: MustNewMoney(1, "INR"), wantErr: ErrMoneyOverflow,
		},
		{
			name: "should reject the sum when it overflows past the int64 minimum",
			a:    MustNewMoney(math.MinInt64, "INR"), b: MustNewMoney(-1, "INR"), wantErr: ErrMoneyOverflow,
		},
		{
			name: "should reject the sum when two large positives overflow",
			a:    MustNewMoney(math.MaxInt64/2+2, "INR"), b: MustNewMoney(math.MaxInt64/2+2, "INR"), wantErr: ErrMoneyOverflow,
		},
		{
			name: "should reject the sum when the currencies differ",
			a:    MustNewMoney(1000, "INR"), b: MustNewMoney(1000, "USD"), wantErr: ErrCurrencyMismatch,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.a.Add(tc.b)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got.AmountMinor())
			assert.Equal(t, tc.a.Currency(), got.Currency())
		})
	}
}

func TestMoneySub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		a, b    Money
		want    int64
		wantErr error
	}{
		{
			name: "should subtract the amounts when both share a currency",
			a:    MustNewMoney(1000, "INR"), b: MustNewMoney(250, "INR"), want: 750,
		},
		{
			name: "should subtract the amounts when the result is negative",
			a:    MustNewMoney(250, "INR"), b: MustNewMoney(1000, "INR"), want: -750,
		},
		{
			name: "should subtract the amounts when subtracting a negative",
			a:    MustNewMoney(250, "INR"), b: MustNewMoney(-250, "INR"), want: 500,
		},
		{
			name: "should reject the difference when subtracting from the int64 minimum",
			a:    MustNewMoney(math.MinInt64, "INR"), b: MustNewMoney(1, "INR"), wantErr: ErrMoneyOverflow,
		},
		{
			// Naive implementations write Sub as Add(-b) and overflow on the
			// negation itself, before any guard has a chance to run.
			name: "should reject the difference when subtracting the int64 minimum",
			a:    MustNewMoney(0, "INR"), b: MustNewMoney(math.MinInt64, "INR"), wantErr: ErrMoneyOverflow,
		},
		{
			name: "should reject the difference when the currencies differ",
			a:    MustNewMoney(1000, "INR"), b: MustNewMoney(1000, "USD"), wantErr: ErrCurrencyMismatch,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.a.Sub(tc.b)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got.AmountMinor())
		})
	}
}

func TestMoneyNeg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      Money
		want    int64
		wantErr error
	}{
		{name: "should negate the amount when it is positive", in: MustNewMoney(1250, "INR"), want: -1250},
		{name: "should negate the amount when it is negative", in: MustNewMoney(-1250, "INR"), want: 1250},
		{name: "should negate the amount when it is zero", in: MustNewMoney(0, "INR"), want: 0},
		{name: "should negate the amount when it is the int64 maximum", in: MustNewMoney(math.MaxInt64, "INR"), want: -math.MaxInt64},
		{
			// int64 is asymmetric: MinInt64 has no positive counterpart, so an
			// unguarded negation returns it unchanged and silently keeps the
			// sign it was meant to flip.
			name: "should reject the negation when the amount is the int64 minimum",
			in:   MustNewMoney(math.MinInt64, "INR"), wantErr: ErrMoneyOverflow,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.in.Neg()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got.AmountMinor())
			assert.Equal(t, tc.in.Currency(), got.Currency())
		})
	}
}

func TestMoneyCmpAndEqual(t *testing.T) {
	t.Parallel()

	inr1000 := MustNewMoney(1000, "INR")
	inr250 := MustNewMoney(250, "INR")
	usd1000 := MustNewMoney(1000, "USD")

	t.Run("should order the amounts when they share a currency", func(t *testing.T) {
		t.Parallel()

		got, err := inr250.Cmp(inr1000)
		require.NoError(t, err)
		assert.Equal(t, -1, got)

		got, err = inr1000.Cmp(inr250)
		require.NoError(t, err)
		assert.Equal(t, 1, got)

		got, err = inr1000.Cmp(MustNewMoney(1000, "INR"))
		require.NoError(t, err)
		assert.Equal(t, 0, got)
	})

	t.Run("should reject the comparison when the currencies differ", func(t *testing.T) {
		t.Parallel()

		_, err := inr1000.Cmp(usd1000)
		require.ErrorIs(t, err, ErrCurrencyMismatch)
	})

	t.Run("should report unequal when the currencies differ", func(t *testing.T) {
		t.Parallel()

		assert.False(t, inr1000.Equal(usd1000))
		assert.True(t, inr1000.Equal(MustNewMoney(1000, "INR")))
	})
}

func TestMoneyScale(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		currency string
		want     int
	}{
		{name: "should use two decimals when the currency is INR", currency: "INR", want: 2},
		{name: "should use two decimals when the currency is USD", currency: "USD", want: 2},
		{name: "should use no decimals when the currency is JPY", currency: "JPY", want: 0},
		{name: "should use no decimals when the currency is KRW", currency: "KRW", want: 0},
		{name: "should use three decimals when the currency is KWD", currency: "KWD", want: 3},
		{name: "should use three decimals when the currency is BHD", currency: "BHD", want: 3},
		{name: "should fall back to two decimals when the currency is unlisted", currency: "ZZZ", want: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, MustNewMoney(1, tc.currency).Scale())
		})
	}
}

func TestMoneyJSON(t *testing.T) {
	t.Parallel()

	t.Run("should emit the amount as a string when marshalling", func(t *testing.T) {
		t.Parallel()

		got, err := json.Marshal(MustNewMoney(1250, "INR"))
		require.NoError(t, err)
		assert.JSONEq(t, `{"amount":"1250","currency":"INR","scale":2}`, string(got))
	})

	t.Run("should emit the currency's own scale when it is not two", func(t *testing.T) {
		t.Parallel()

		got, err := json.Marshal(MustNewMoney(1000, "JPY"))
		require.NoError(t, err)
		assert.JSONEq(t, `{"amount":"1000","currency":"JPY","scale":0}`, string(got))
	})

	t.Run("should reject the value when marshalling the zero Money", func(t *testing.T) {
		t.Parallel()

		_, err := json.Marshal(Money{})
		require.ErrorIs(t, err, ErrInvalidCurrency)
	})

	t.Run("should survive a round trip past the float64 precision limit", func(t *testing.T) {
		t.Parallel()

		// 2^53 + 1 is the smallest integer JavaScript cannot represent. This is
		// the entire reason amount is carried as a string.
		const beyondFloat64 = int64(1)<<53 + 1

		encoded, err := json.Marshal(MustNewMoney(beyondFloat64, "INR"))
		require.NoError(t, err)

		var decoded Money
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		assert.Equal(t, beyondFloat64, decoded.AmountMinor())
	})

	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr error
	}{
		{
			name:  "should decode the amount when it is a string",
			input: `{"amount":"1250","currency":"INR","scale":2}`, want: 1250,
		},
		{
			name:  "should decode the amount when scale is omitted",
			input: `{"amount":"1250","currency":"INR"}`, want: 1250,
		},
		{
			name:  "should decode the amount when it is negative",
			input: `{"amount":"-1250","currency":"INR"}`, want: -1250,
		},
		{
			name:  "should reject the amount when it is a JSON number",
			input: `{"amount":1250,"currency":"INR","scale":2}`, wantErr: ErrInvalidEntry,
		},
		{
			name:  "should reject the amount when it is a JSON float",
			input: `{"amount":12.5,"currency":"INR","scale":2}`, wantErr: ErrInvalidEntry,
		},
		{
			name:  "should reject the amount when the string holds a decimal point",
			input: `{"amount":"12.50","currency":"INR","scale":2}`, wantErr: ErrInvalidEntry,
		},
		{
			name:  "should reject the amount when it exceeds int64",
			input: `{"amount":"9223372036854775808","currency":"INR"}`, wantErr: ErrMoneyOverflow,
		},
		{
			name:  "should reject the value when the currency is malformed",
			input: `{"amount":"1250","currency":"rupees"}`, wantErr: ErrInvalidCurrency,
		},
		{
			// 1250 paise and 1250 rupees are indistinguishable once accepted, so
			// a disagreement about scale has to be an error rather than a
			// preference.
			name:  "should reject the value when the scale contradicts the currency",
			input: `{"amount":"1250","currency":"INR","scale":0}`, wantErr: ErrScaleMismatch,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got Money
			err := json.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got.AmountMinor())
		})
	}
}

// TestMoneyArithmeticNeverPanics runs the operations across the awkward corners
// of int64 to prove that every path either returns a value or an error.
func TestMoneyArithmeticNeverPanics(t *testing.T) {
	t.Parallel()

	corners := []int64{
		math.MinInt64, math.MinInt64 + 1, -1 << 53, -1, 0, 1, 1 << 53,
		math.MaxInt64 - 1, math.MaxInt64,
	}

	for _, a := range corners {
		for _, b := range corners {
			x := MustNewMoney(a, "INR")
			y := MustNewMoney(b, "INR")

			if sum, err := x.Add(y); err == nil {
				assert.Equal(t, a+b, sum.AmountMinor(), "add %d + %d", a, b)
			}
			if diff, err := x.Sub(y); err == nil {
				assert.Equal(t, a-b, diff.AmountMinor(), "sub %d - %d", a, b)
			}
			if neg, err := x.Neg(); err == nil {
				assert.Equal(t, -a, neg.AmountMinor(), "neg %d", a)
			}
		}
	}
}
