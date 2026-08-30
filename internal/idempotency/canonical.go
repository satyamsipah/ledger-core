package idempotency

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
)

// Canonicalize rewrites a JSON document into RFC 8785 (JSON Canonicalization
// Scheme) form, so that two bodies a client considers identical hash
// identically.
//
// # WHY RFC 8785 AND NOT A HAND-ROLLED SORTER
//
// "Sort the keys and strip the whitespace" is the obvious implementation and it
// is wrong in three places that matter. Object key order has to be defined over
// UTF-16 code units, not Go's byte order, or two keys outside the Basic
// Multilingual Plane sort differently here than in the browser that sent them.
// Numbers have to be reduced to a single form, or 1250 and 1.25e3 -- the same
// value, and the same value to every JSON parser in existence -- fingerprint
// differently and a legitimate retry gets a 422. And string escaping has to be
// pinned exactly, or "A" and "A" diverge. JCS specifies all three. Writing
// it out is a day's work; discovering any one of them in production is worse.
//
// Duplicate object keys are REJECTED rather than resolved. Go's encoding/json
// silently keeps the last, other parsers keep the first, and a proxy in between
// may keep either -- so a document containing them has no single meaning, and a
// fingerprint over a document with no single meaning is not a security control.
//
// An empty or whitespace-only body canonicalizes to the empty string. That is
// deliberate rather than an error: a POST with no body is a legitimate request
// shape, and it has exactly one canonical form.
func Canonicalize(raw []byte) ([]byte, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return []byte{}, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	value, err := parseValue(decoder)
	if err != nil {
		return nil, err
	}

	// A document with trailing content is two documents. Accepting the first and
	// ignoring the rest would let a caller append anything at all without
	// changing the fingerprint, which turns the fingerprint into a suggestion.
	if _, err := decoder.Token(); !errorIsEOF(err) {
		return nil, fmt.Errorf("canonicalize: trailing content after the top-level JSON value: %w", ErrMalformedBody)
	}

	var out bytes.Buffer
	if err := writeValue(&out, value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// canonicalObject preserves nothing about input order -- it exists only to
// detect duplicate keys while parsing, which is information the map alone
// cannot carry.
type canonicalObject struct {
	keys   []string
	values map[string]any
}

func parseValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("canonicalize: %v: %w", err, ErrMalformedBody)
	}
	return parseFromToken(decoder, token)
}

func parseFromToken(decoder *json.Decoder, token json.Token) (any, error) {
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		// Scalars arrive fully formed: nil, bool, string, or json.Number because
		// the decoder was put in UseNumber mode. Nothing here needs converting,
		// and converting a number to float64 at this point would discard the
		// literal before writeNumber has had a chance to canonicalize it.
		return token, nil
	}

	switch delim {
	case '{':
		object := &canonicalObject{values: map[string]any{}}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, fmt.Errorf("canonicalize object key: %v: %w", err, ErrMalformedBody)
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("canonicalize: object key %v is not a string: %w", keyToken, ErrMalformedBody)
			}
			if _, duplicate := object.values[key]; duplicate {
				return nil, fmt.Errorf("canonicalize: duplicate object key %q: %w", key, ErrMalformedBody)
			}

			value, err := parseValue(decoder)
			if err != nil {
				return nil, err
			}
			object.keys = append(object.keys, key)
			object.values[key] = value
		}
		if _, err := decoder.Token(); err != nil { // consume '}'
			return nil, fmt.Errorf("canonicalize: %v: %w", err, ErrMalformedBody)
		}
		return object, nil

	case '[':
		// Array order is data, not formatting, so it is preserved untouched.
		// Sorting arrays here would make [1,2] and [2,1] the same request, and
		// the order of a transaction's entries is precisely what decides which
		// account is debited.
		items := []any{}
		for decoder.More() {
			value, err := parseValue(decoder)
			if err != nil {
				return nil, err
			}
			items = append(items, value)
		}
		if _, err := decoder.Token(); err != nil { // consume ']'
			return nil, fmt.Errorf("canonicalize: %v: %w", err, ErrMalformedBody)
		}
		return items, nil

	default:
		return nil, fmt.Errorf("canonicalize: unexpected %q: %w", delim, ErrMalformedBody)
	}
}

func writeValue(out *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if v {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		writeString(out, v)
	case json.Number:
		return writeNumber(out, v)
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := writeValue(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case *canonicalObject:
		return writeObject(out, v)
	default:
		return fmt.Errorf("canonicalize: unsupported value of type %T: %w", value, ErrMalformedBody)
	}
	return nil
}

func writeObject(out *bytes.Buffer, object *canonicalObject) error {
	keys := make([]string, len(object.keys))
	copy(keys, object.keys)

	// RFC 8785 orders keys by their UTF-16 code units, which is what JavaScript's
	// Array.prototype.sort does to the output of Object.keys. Go's natural string
	// comparison is over UTF-8 bytes, and the two disagree for anything above the
	// BMP: U+10000 encodes as the surrogate pair D800 DC00 in UTF-16, which sorts
	// BELOW U+E000 there and ABOVE it in UTF-8. Rare in a payments API and
	// completely silent when wrong, which is the combination worth handling.
	sort.SliceStable(keys, func(i, j int) bool {
		return lessUTF16(keys[i], keys[j])
	})

	out.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			out.WriteByte(',')
		}
		writeString(out, key)
		out.WriteByte(':')
		if err := writeValue(out, object.values[key]); err != nil {
			return err
		}
	}
	out.WriteByte('}')
	return nil
}

func lessUTF16(a, b string) bool {
	left, right := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(left) && i < len(right); i++ {
		if left[i] != right[i] {
			return left[i] < right[i]
		}
	}
	return len(left) < len(right)
}

// writeString emits a JSON string using the shortest escape for every character
// that needs one, per RFC 8785 section 3.2.2.2.
//
// Written by hand rather than delegating to encoding/json because that package
// escapes <, > and & into <-style sequences for HTML safety, and escapes
// U+2028 and U+2029 for JavaScript safety. Both are sensible defaults for a
// browser and both are non-canonical here: the same string would hash
// differently depending on which library produced it.
func writeString(out *bytes.Buffer, s string) {
	out.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if r < 0x20 {
				out.WriteString(`\u`)
				const hex = "0123456789abcdef"
				out.WriteByte('0')
				out.WriteByte('0')
				out.WriteByte(hex[(r>>4)&0xF])
				out.WriteByte(hex[r&0xF])
				continue
			}
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
}

// writeNumber emits a number in the single form ECMAScript's Number::toString
// would produce, which is what RFC 8785 requires.
//
// This is the part of canonicalization that a naive implementation gets wrong.
// 1250, 1.25e3, 1250.0 and 1.250e+3 are one value to every JSON parser, so a
// client library that renders a metadata field in exponential form must not
// turn a retry into a 422. Preserving the input literal instead -- the obvious
// shortcut, since UseNumber hands it over for free -- would do exactly that.
//
// Money is not affected either way: ledger.Money is a JSON string on the wire
// precisely so that no amount is ever subject to float64 rounding (see
// money.go). This path exists for transactions.metadata, which is arbitrary
// client JSON and can hold numbers.
func writeNumber(out *bytes.Buffer, number json.Number) error {
	f, err := strconv.ParseFloat(number.String(), 64)
	if err != nil {
		return fmt.Errorf("canonicalize number %q: %v: %w", number.String(), err, ErrMalformedBody)
	}

	// ECMAScript renders both zeroes as "0"; -0 and 0 are the same value to ==,
	// so they must be the same request.
	if f == 0 {
		out.WriteString("0")
		return nil
	}

	if f < 0 {
		out.WriteByte('-')
		f = -f
	}

	// 'e' with precision -1 gives the shortest digit string that round-trips,
	// which is the same digit string ECMAScript starts from. Everything below is
	// deciding where the decimal point goes.
	formatted := strconv.FormatFloat(f, 'e', -1, 64)
	mantissa, exponent, ok := strings.Cut(formatted, "e")
	if !ok {
		return fmt.Errorf("canonicalize number %q: unexpected format %q: %w",
			number.String(), formatted, ErrMalformedBody)
	}
	digits := strings.Replace(mantissa, ".", "", 1)
	exp10, err := strconv.Atoi(exponent)
	if err != nil {
		return fmt.Errorf("canonicalize number %q: exponent %q: %w", number.String(), exponent, ErrMalformedBody)
	}

	// k is the digit count and n the position of the decimal point relative to
	// the start of the digits, matching the variable names in the ECMAScript
	// specification of Number::toString so the four branches can be checked
	// against it directly.
	k := len(digits)
	n := exp10 + 1

	switch {
	case k <= n && n <= 21:
		out.WriteString(digits)
		out.WriteString(strings.Repeat("0", n-k))
	case 0 < n && n <= 21:
		out.WriteString(digits[:n])
		out.WriteByte('.')
		out.WriteString(digits[n:])
	case -6 < n && n <= 0:
		out.WriteString("0.")
		out.WriteString(strings.Repeat("0", -n))
		out.WriteString(digits)
	default:
		out.WriteString(digits[:1])
		if k > 1 {
			out.WriteByte('.')
			out.WriteString(digits[1:])
		}
		out.WriteByte('e')
		if n-1 >= 0 {
			out.WriteByte('+')
		}
		out.WriteString(strconv.Itoa(n - 1))
	}
	return nil
}

func errorIsEOF(err error) bool { return err == io.EOF }
