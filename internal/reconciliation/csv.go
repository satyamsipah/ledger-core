package reconciliation

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// pspStatementColumns is the required header set. Order does not matter --
// matched by name, not position, so a PSP that reorders its own export
// columns does not break this parser -- but every one of them must be
// present, checked against exactly this set rather than assumed from
// whatever happens to be in row 1.
var pspStatementColumns = []string{"external_ref", "amount_minor", "currency", "status", "settled_at"}

// validPSPStatuses is the fixed vocabulary this package understands on the
// PSP side of a settlement. statusesAgree in classify.go is defined only over
// these four, so accepting anything else here would let a row through that no
// classification rule can ever agree with -- silently becoming a permanent
// STATUS_MISMATCH instead of a clear parse error naming the actual problem: a
// PSP that introduced a status value this build has never been taught.
var validPSPStatuses = map[string]bool{
	"SETTLED":  true,
	"FAILED":   true,
	"REFUNDED": true,
	"PENDING":  true,
}

// ParsePSPStatement reads a mock PSP settlement file: external_ref,
// amount_minor, currency, status, settled_at (RFC 3339), one row per
// settlement, columns in any order.
//
// Validation happens here, once, rather than being left to the SQL match
// query -- a malformed row should fail the whole run with a clear line
// number, not surface as a cryptic type error from a database function or,
// worse, be silently coerced into something that produces a wrong report.
//
// currency is validated through ledger.NewMoney rather than a second regex:
// D2 in docs/DECISIONS.md already settled what a valid currency code is, and
// this file should reject exactly what the ledger itself would reject, not
// its own approximation of that rule.
func ParsePSPStatement(r io.Reader) ([]PSPRecord, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, ErrEmptyStatement
		}
		return nil, fmt.Errorf("read PSP statement header: %w", err)
	}
	index, err := columnIndex(header)
	if err != nil {
		return nil, err
	}

	var records []PSPRecord
	for line := 2; ; line++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read PSP statement row %d: %w", line, err)
		}
		if len(row) != len(header) {
			return nil, fmt.Errorf("PSP statement row %d has %d columns, want %d: %w",
				line, len(row), len(header), ErrMalformedRecord)
		}

		record, err := parsePSPRow(row, index)
		if err != nil {
			return nil, fmt.Errorf("PSP statement row %d: %w", line, err)
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		return nil, ErrEmptyStatement
	}
	return records, nil
}

// columnIndex maps each required column name to its position in header,
// rejecting a header missing one of them. A duplicated column name is not
// specifically detected: the last occurrence wins, which is no worse than
// what any positional parser would have done with a malformed header.
func columnIndex(header []string) (map[string]int, error) {
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[name] = i
	}
	for _, want := range pspStatementColumns {
		if _, ok := index[want]; !ok {
			return nil, fmt.Errorf("PSP statement header is missing column %q: %w", want, ErrMalformedRecord)
		}
	}
	return index, nil
}

func parsePSPRow(row []string, index map[string]int) (PSPRecord, error) {
	externalRef := row[index["external_ref"]]
	amountRaw := row[index["amount_minor"]]
	currency := row[index["currency"]]
	status := strings.ToUpper(row[index["status"]])
	settledAtRaw := row[index["settled_at"]]

	if externalRef == "" {
		return PSPRecord{}, fmt.Errorf("external_ref is required: %w", ErrMalformedRecord)
	}
	if !validPSPStatuses[status] {
		return PSPRecord{}, fmt.Errorf("status %q is not recognised: %w", status, ErrMalformedRecord)
	}

	amountMinor, err := strconv.ParseInt(amountRaw, 10, 64)
	if err != nil {
		return PSPRecord{}, fmt.Errorf("amount_minor %q is not an integer: %w", amountRaw, ErrMalformedRecord)
	}
	if amountMinor <= 0 {
		return PSPRecord{}, fmt.Errorf("amount_minor %d must be positive: %w", amountMinor, ErrMalformedRecord)
	}

	// Discarded: only the currency validation is wanted here. Building the
	// Money is still the right way to get it, rather than a second regex that
	// could silently drift from what NewMoney actually accepts.
	if _, err := ledger.NewMoney(amountMinor, currency); err != nil {
		return PSPRecord{}, fmt.Errorf("currency %q: %w", currency, ErrMalformedRecord)
	}

	settledAt, err := time.Parse(time.RFC3339, settledAtRaw)
	if err != nil {
		return PSPRecord{}, fmt.Errorf("settled_at %q is not RFC3339: %w", settledAtRaw, ErrMalformedRecord)
	}

	return PSPRecord{
		ExternalRef: externalRef,
		AmountMinor: amountMinor,
		Currency:    currency,
		Status:      status,
		SettledAt:   settledAt,
	}, nil
}
