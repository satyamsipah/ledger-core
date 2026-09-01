package idempotency

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Control characters and the JavaScript line separators are invisible in an
// editor and survive copy-paste unpredictably, so no test below embeds one
// directly. JSON escape sequences are assembled by u() and real code points by
// string(rune(...)), which keeps this file pure ASCII and makes each test state
// exactly which code point it means.
func u(hex string) string { return "\\u" + hex }

func TestCanonicalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "should sort object keys when they arrive out of order",
			in:   `{"c":1,"a":2,"b":3}`,
			want: `{"a":2,"b":3,"c":1}`,
		},
		{
			name: "should strip insignificant whitespace when the body is pretty-printed",
			in:   "{\n  \"a\" : 1,\n  \"b\" : [ 1, 2 ]\n}",
			want: `{"a":1,"b":[1,2]}`,
		},
		{
			name: "should sort keys recursively when objects are nested",
			in:   `{"outer":{"z":1,"a":{"y":2,"b":3}}}`,
			want: `{"outer":{"a":{"b":3,"y":2},"z":1}}`,
		},
		{
			// Array order is data, not formatting. Sorting here would make
			// [1,2] and [2,1] the same request, and the order of a
			// transaction's entries decides which account is debited.
			name: "should preserve array order when elements could be sorted",
			in:   `{"entries":[{"seq":1},{"seq":0}]}`,
			want: `{"entries":[{"seq":1},{"seq":0}]}`,
		},
		{
			name: "should render an empty object and array unchanged",
			in:   `{"o":{},"a":[]}`,
			want: `{"a":[],"o":{}}`,
		},
		{
			name: "should canonicalize the empty body to the empty string",
			in:   "   \n\t ",
			want: ``,
		},
		{
			name: "should keep a top-level scalar",
			in:   `  "hello"  `,
			want: `"hello"`,
		},
		{
			name: "should preserve null and booleans",
			in:   `{"a":null,"b":true,"c":false}`,
			want: `{"a":null,"b":true,"c":false}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Canonicalize([]byte(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(got))
		})
	}
}

// TestCanonicalize_NumbersReduceToOneForm is the case a hand-rolled
// canonicalizer gets wrong.
//
// Every input in a group is the same IEEE-754 double and therefore the same
// value to every JSON parser in existence. If they canonicalize differently, a
// client library that happens to render a metadata number in exponential form
// turns a legitimate retry into a 422, and the bug stays invisible until some
// client somewhere upgrades its JSON encoder.
func TestCanonicalize_NumbersReduceToOneForm(t *testing.T) {
	t.Parallel()

	groups := []struct {
		name   string
		inputs []string
		want   string
	}{
		{
			name:   "should render an integer identically however it was written",
			inputs: []string{`1250`, `1.25e3`, `1250.0`, `1.250E+3`, `12.50e2`},
			want:   `1250`,
		},
		{
			name:   "should collapse both zeroes",
			inputs: []string{`0`, `-0`, `0.0`, `0e10`},
			want:   `0`,
		},
		{
			name:   "should keep a fraction in plain form",
			inputs: []string{`0.5`, `5e-1`, `0.50`, `50e-2`},
			want:   `0.5`,
		},
		{
			name:   "should switch to exponential at 1e21, matching ECMAScript",
			inputs: []string{`1e21`, `1000000000000000000000`},
			want:   `1e+21`,
		},
		{
			name:   "should stay in plain form just below the 1e21 boundary",
			inputs: []string{`1e20`},
			want:   `100000000000000000000`,
		},
		{
			name:   "should stay in plain form at 1e-6",
			inputs: []string{`1e-6`, `0.000001`},
			want:   `0.000001`,
		},
		{
			name:   "should switch to exponential at 1e-7, matching ECMAScript",
			inputs: []string{`1e-7`, `0.0000001`},
			want:   `1e-7`,
		},
		{
			name:   "should carry the sign through",
			inputs: []string{`-1.5`, `-15e-1`},
			want:   `-1.5`,
		},
	}

	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			t.Parallel()
			for _, in := range group.inputs {
				got, err := Canonicalize([]byte(`{"n":` + in + `}`))
				require.NoError(t, err, "input %s", in)
				assert.Equal(t, `{"n":`+group.want+`}`, string(got), "input %s", in)
			}
		})
	}
}

func TestCanonicalize_Strings(t *testing.T) {
	t.Parallel()

	lineSep := string(rune(0x2028))
	paraSep := string(rune(0x2029))
	eAcute := string(rune(0x00E9))

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "should use the short escape for a control character that has one",
			in:   `{"a":"x` + u("000A") + `y"}`,
			want: `{"a":"x\ny"}`,
		},
		{
			name: "should use a lower-case hex escape for a control character with no short form",
			in:   `{"a":"x` + u("0001") + `y"}`,
			want: `{"a":"x` + u("0001") + `y"}`,
		},
		{
			name: "should normalise the case of a hex escape",
			in:   `{"a":"x` + u("000F") + `y"}`,
			want: `{"a":"x` + u("000f") + `y"}`,
		},
		{
			// encoding/json emits \u003c, \u003e and \u0026 here for HTML safety.
			// Canonical output must not, or the same string hashes differently
			// depending on which library produced it.
			name: "should leave HTML-significant characters unescaped, unlike encoding/json",
			in:   `{"a":"<b>&</b>"}`,
			want: `{"a":"<b>&</b>"}`,
		},
		{
			// The other place encoding/json deviates: it escapes U+2028 and
			// U+2029 so its output is safe to paste inside a <script> tag.
			name: "should leave the JavaScript line separators unescaped, unlike encoding/json",
			in:   `{"a":"x` + u("2028") + `y` + u("2029") + `z"}`,
			want: `{"a":"x` + lineSep + `y` + paraSep + `z"}`,
		},
		{
			name: "should decode an escaped character to its literal form",
			in:   `{"a":"` + u("00e9") + `"}`,
			want: `{"a":"` + eAcute + `"}`,
		},
		{
			name: "should escape a quote and a backslash",
			in:   `{"a":"q\"b\\"}`,
			want: `{"a":"q\"b\\"}`,
		},
		{
			name: "should leave a forward slash unescaped when it arrived escaped",
			in:   `{"a":"a\/b"}`,
			want: `{"a":"a/b"}`,
		},
		{
			// Go compares strings by UTF-8 bytes; RFC 8785 orders keys by UTF-16
			// code units. They agree here, and the test exists so the more
			// interesting disagreement above the BMP has a companion.
			name: "should sort keys by code unit when they are not all ASCII",
			in:   `{"` + u("00e9") + `":1,"z":2,"a":3}`,
			want: `{"a":3,"z":2,"` + eAcute + `":1}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Canonicalize([]byte(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(got))
		})
	}
}

func TestCanonicalize_Rejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
	}{
		{
			// Go keeps the last, other parsers keep the first, and a proxy in
			// between may keep either -- so the document has no single meaning,
			// and fingerprinting a document with no single meaning is not a
			// control.
			name: "should reject a duplicate object key",
			in:   `{"a":1,"a":2}`,
		},
		{
			name: "should reject a duplicate key nested inside an object",
			in:   `{"outer":{"a":1,"a":2}}`,
		},
		{
			// Accepting the first document and ignoring the rest would let a
			// caller append anything at all without changing the fingerprint.
			name: "should reject trailing content after the top-level value",
			in:   `{"a":1} {"b":2}`,
		},
		{
			name: "should reject trailing junk after the top-level value",
			in:   `{"a":1}xyz`,
		},
		{
			name: "should reject malformed JSON",
			in:   `{"a":`,
		},
		{
			name: "should reject an unterminated array",
			in:   `[1,2`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Canonicalize([]byte(tc.in))
			require.ErrorIs(t, err, ErrMalformedBody)
		})
	}
}

// TestCanonicalize_RFC8785Example runs the worked example from section 3.2.3 of
// the JCS specification, so the implementation is checked against the standard
// rather than only against itself.
func TestCanonicalize_RFC8785Example(t *testing.T) {
	t.Parallel()

	in := `{
		"numbers": [333333333.33333329, 1E30, 4.50, 2e-3, 0.000000000000000000000000001],
		"string": "` + u("20ac") + `$` + u("000F") + u("000a") + `A'` + u("0042") +
		u("0022") + u("005c") + `\\\"\/",
		"literals": [null, true, false]
	}`

	want := `{"literals":[null,true,false],` +
		`"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],` +
		`"string":"` + string(rune(0x20AC)) + `$` + u("000f") + `\nA'B\"\\\\\"/"}`

	got, err := Canonicalize([]byte(in))
	require.NoError(t, err)
	assert.Equal(t, want, string(got))
}

// TestCanonicalize_IsIdempotent asserts that canonicalizing already-canonical
// output changes nothing. A canonicalizer that is not a fixed point would make
// a fingerprint depend on how many times the body had been normalised on its
// way in.
func TestCanonicalize_IsIdempotent(t *testing.T) {
	t.Parallel()

	const in = `{"z":[1.50,{"b":2,"a":null}],"a":"x\ty","n":1e3}`

	once, err := Canonicalize([]byte(in))
	require.NoError(t, err)

	twice, err := Canonicalize(once)
	require.NoError(t, err)

	assert.Equal(t, string(once), string(twice))
}
