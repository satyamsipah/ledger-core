package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
)

// currencyPattern mirrors accounts_currency_check in migration 000002. Keeping
// the same rule on both sides means a currency rejected by the database is
// rejected here first, with a domain error instead of a constraint violation.
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// defaultScale is the ISO-4217 minor-unit exponent for the overwhelming
// majority of currencies. Only the exceptions are listed in currencyScale.
const defaultScale = 2

// currencyScale holds the currencies whose ISO-4217 exponent is not 2.
//
// This table only ever affects presentation: amounts are stored, summed and
// compared in minor units, and scale never enters the arithmetic. It exists so
// a client knows where to put the decimal point without hard-coding "divide by
// 100" and quietly displaying 1000 yen as ¥10.00.
var currencyScale = map[string]int{
	// Zero-decimal currencies: the minor unit is the major unit.
	"BIF": 0, "CLP": 0, "DJF": 0, "GNF": 0, "ISK": 0, "JPY": 0, "KMF": 0,
	"KRW": 0, "PYG": 0, "RWF": 0, "UGX": 0, "UYI": 0, "VND": 0, "VUV": 0,
	"XAF": 0, "XOF": 0, "XPF": 0,

	// Three-decimal currencies, mostly Gulf dinars.
	"BHD": 3, "IQD": 3, "JOD": 3, "KWD": 3, "LYD": 3, "OMR": 3, "TND": 3,

	// Four-decimal units used for indexation rather than for payments.
	"CLF": 4, "UYW": 4,
}

// Money is an exact amount in the minor units of one currency.
//
// Both fields are unexported and there is no way to construct a Money holding a
// float, because invariant 3 is not a coding preference: 0.1 + 0.2 is not 0.3
// in binary floating point, and a ledger that loses a paisa per thousand
// transactions fails reconciliation in a way that takes weeks to trace back to
// its cause. Every operation here is integer arithmetic with an explicit
// overflow check.
//
// The zero value is deliberately not usable -- it carries no currency, so
// Validate and MarshalJSON both reject it. That turns "someone forgot to
// initialise this" into an error at the boundary rather than a silent
// zero-rupee entry.
type Money struct {
	amountMinor int64
	currency    string
}

// NewMoney builds an amount in minor units. The currency is validated here
// rather than at the database boundary so that a malformed code fails before it
// has been written into a half-built transaction.
func NewMoney(amountMinor int64, currency string) (Money, error) {
	if !currencyPattern.MatchString(currency) {
		return Money{}, fmt.Errorf("currency %q: %w", currency, ErrInvalidCurrency)
	}
	return Money{amountMinor: amountMinor, currency: currency}, nil
}

// MustNewMoney is NewMoney for values fixed at compile time -- test tables,
// constants, seed data. It panics rather than returning an error because the
// only inputs it is meant for are ones a human already typed correctly, and
// threading an impossible error through a var block obscures the code that
// matters.
func MustNewMoney(amountMinor int64, currency string) Money {
	m, err := NewMoney(amountMinor, currency)
	if err != nil {
		panic(err)
	}
	return m
}

// AmountMinor returns the amount in minor units. This is the only numeric
// accessor: there is no Float64 method, and adding one would defeat the point
// of the type.
func (m Money) AmountMinor() int64 { return m.amountMinor }

// Currency returns the ISO-4217 code.
func (m Money) Currency() string { return m.currency }

// Scale returns the currency's ISO-4217 minor-unit exponent, for callers
// rendering the amount. It is not used in arithmetic anywhere in this package.
func (m Money) Scale() int { return scaleFor(m.currency) }

// IsZero reports whether the amount is zero, regardless of currency.
func (m Money) IsZero() bool { return m.amountMinor == 0 }

// IsNegative reports whether the amount is below zero. Balances may legitimately
// be negative; journal entry amounts may not.
func (m Money) IsNegative() bool { return m.amountMinor < 0 }

// Validate reports whether the value is usable, catching the zero Money.
func (m Money) Validate() error {
	if !currencyPattern.MatchString(m.currency) {
		return fmt.Errorf("currency %q: %w", m.currency, ErrInvalidCurrency)
	}
	return nil
}

// Add returns m + other.
//
// It refuses to add across currencies rather than picking one, because there is
// no correct answer: adding 100 INR to 100 USD is not 200 of anything, and any
// implicit conversion would bury an exchange rate somewhere no one audits.
func (m Money) Add(other Money) (Money, error) {
	if err := m.sameCurrency(other); err != nil {
		return Money{}, err
	}
	sum, ok := addInt64(m.amountMinor, other.amountMinor)
	if !ok {
		return Money{}, fmt.Errorf("add %s to %s: %w", other, m, ErrMoneyOverflow)
	}
	return Money{amountMinor: sum, currency: m.currency}, nil
}

// Sub returns m - other, under the same currency rule as Add.
func (m Money) Sub(other Money) (Money, error) {
	if err := m.sameCurrency(other); err != nil {
		return Money{}, err
	}
	diff, ok := subInt64(m.amountMinor, other.amountMinor)
	if !ok {
		return Money{}, fmt.Errorf("subtract %s from %s: %w", other, m, ErrMoneyOverflow)
	}
	return Money{amountMinor: diff, currency: m.currency}, nil
}

// Neg returns -m.
//
// It can fail, which looks surprising until you notice that int64 is asymmetric:
// there is no positive counterpart to math.MinInt64, so negating it wraps back
// to itself. A Neg that silently returned a still-negative number would turn a
// credit into a credit and unbalance the transaction that used it.
func (m Money) Neg() (Money, error) {
	if m.amountMinor == math.MinInt64 {
		return Money{}, fmt.Errorf("negate %s: %w", m, ErrMoneyOverflow)
	}
	return Money{amountMinor: -m.amountMinor, currency: m.currency}, nil
}

// Cmp compares two amounts of the same currency, returning -1, 0 or 1.
func (m Money) Cmp(other Money) (int, error) {
	if err := m.sameCurrency(other); err != nil {
		return 0, err
	}
	switch {
	case m.amountMinor < other.amountMinor:
		return -1, nil
	case m.amountMinor > other.amountMinor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports whether both the amount and the currency match. Unlike Cmp it
// cannot fail: two amounts in different currencies are simply not equal.
func (m Money) Equal(other Money) bool {
	return m.amountMinor == other.amountMinor && m.currency == other.currency
}

// String renders minor units and currency without inventing a decimal point,
// so log lines and error messages cannot be misread as major units.
func (m Money) String() string {
	return strconv.FormatInt(m.amountMinor, 10) + " " + m.currency
}

func (m Money) sameCurrency(other Money) error {
	if m.currency != other.currency {
		return fmt.Errorf("%s and %s: %w", m.currency, other.currency, ErrCurrencyMismatch)
	}
	return nil
}

// moneyJSON is the wire shape: {"amount":"1250","currency":"INR","scale":2}.
//
// amount is a STRING, and that is the whole reason this type has custom JSON
// methods. The admin dashboard is TypeScript, where every JSON number becomes a
// float64: amounts past 2^53 minor units lose precision silently on the way
// through JSON.parse, and a ledger whose largest values are the ones that
// corrupt is a ledger that fails first on its most important rows.
type moneyJSON struct {
	Amount   json.RawMessage `json:"amount"`
	Currency string          `json:"currency"`
	Scale    *int            `json:"scale,omitempty"`
}

// MarshalJSON emits the amount as a decimal string in minor units.
func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("marshal money: %w", err)
	}
	scale := m.Scale()
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
		Scale    int    `json:"scale"`
	}{
		Amount:   strconv.FormatInt(m.amountMinor, 10),
		Currency: m.currency,
		Scale:    scale,
	})
}

// UnmarshalJSON decodes the wire shape, rejecting a JSON number for amount.
//
// The rejection is the point. Accepting a number would let a client send
// 12.5 -- or 9007199254740993, which JavaScript has already rounded -- and have
// it land in the ledger looking like a whole, exact value. Refusing the type
// outright is the only check that cannot be fooled by a value that happens to
// round-trip cleanly today.
func (m *Money) UnmarshalJSON(data []byte) error {
	var raw moneyJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decode money: %w", err)
	}

	if len(raw.Amount) == 0 || raw.Amount[0] != '"' {
		return fmt.Errorf("decode money: amount must be a JSON string in minor units, got %s: %w",
			string(raw.Amount), ErrInvalidEntry)
	}

	var amountStr string
	if err := json.Unmarshal(raw.Amount, &amountStr); err != nil {
		return fmt.Errorf("decode money amount: %w", err)
	}

	amountMinor, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil {
		// ParseInt reports out-of-range separately from malformed input, and the
		// two mean different things to a caller: one is a value too large for
		// the ledger, the other is not a number at all. errors.Is rather than a
		// type assertion on *strconv.NumError, so the distinction survives any
		// future wrapping.
		if errors.Is(err, strconv.ErrRange) {
			return fmt.Errorf("decode money amount %q: %w", amountStr, ErrMoneyOverflow)
		}
		return fmt.Errorf("decode money amount %q: %w", amountStr, ErrInvalidEntry)
	}

	decoded, err := NewMoney(amountMinor, raw.Currency)
	if err != nil {
		return fmt.Errorf("decode money: %w", err)
	}

	// A scale that disagrees with the currency means the sender and this ledger
	// disagree about what the amount counts. Rejecting is the only safe answer:
	// there is no way to tell 1250 paise from 1250 rupees after the fact.
	if raw.Scale != nil && *raw.Scale != decoded.Scale() {
		return fmt.Errorf("decode money: scale %d for %s, expected %d: %w",
			*raw.Scale, decoded.currency, decoded.Scale(), ErrScaleMismatch)
	}

	*m = decoded
	return nil
}

func scaleFor(currency string) int {
	if scale, ok := currencyScale[currency]; ok {
		return scale
	}
	return defaultScale
}

// addInt64 returns a+b and whether it fits in int64.
//
// Overflow is only possible when both operands share a sign, and it always
// shows up as a result whose sign disagrees with them. Checking after the fact
// is safe because signed overflow in Go is defined to wrap, unlike in C.
func addInt64(a, b int64) (int64, bool) {
	sum := a + b
	if (a > 0 && b > 0 && sum < 0) || (a < 0 && b < 0 && sum >= 0) {
		return 0, false
	}
	return sum, true
}

// subInt64 returns a-b and whether it fits in int64.
//
// Written directly rather than as addInt64(a, -b) because -b is itself
// unrepresentable when b is math.MinInt64, which would make the guard the first
// thing to overflow.
func subInt64(a, b int64) (int64, bool) {
	diff := a - b
	if (b < 0 && diff < a) || (b > 0 && diff > a) {
		return 0, false
	}
	return diff, true
}
