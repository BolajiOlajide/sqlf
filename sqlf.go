// Package sqlf generates parameterized SQL statements in Go, sprintf style.
//
// A simple example:
//
//	q := sqlf.Sprintf("SELECT * FROM users WHERE country = %s AND age > %d", "US", 27);
//	rows, err := db.Query(q.Query(sqlf.SimpleBindVar), q.Args()...) // db is a database/sql.DB
//
// sqlf.Sprintf does not return a string. It returns *sqlf.Query which has
// methods for a parameterized SQL query and arguments. You then pass that to
// db.Query, db.Exec, etc. This is not like using fmt.Sprintf, which could
// expose you to malformed SQL or SQL injection attacks.
//
// sqlf.Query can be passed as an argument to sqlf.Sprintf. It will "flatten"
// the query string, while preserving the correct variable binding. This
// allows you to easily compose and build SQL queries. See the below examples
// to find out more.
package sqlf

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Query stores a SQL expression and arguments for passing on to
// database/sql/db.Query or gorp.SqlExecutor.
type Query struct {
	fmt  string
	args []interface{}
	// argIndices maps each placeholder position to its argument index.
	// This supports explicit argument indexing like %[1]s.
	// If nil, placeholders map 1:1 to args (legacy behavior).
	argIndices []int
}

// directive represents a parsed fmt directive in a format string.
type directive struct {
	start    int  // start position in format string
	end      int  // end position (exclusive)
	hasIndex bool // whether an explicit index [n] was present
	index    int  // the explicit index (1-based), or 0 if implicit
	isLiteral bool // true if this is %% (literal percent)
}

// skipOptionalIndex skips an optional explicit index [n] at position i in format.
// Returns the new position after skipping.
func skipOptionalIndex(format string, i int) int {
	if i < len(format) && format[i] == '[' {
		j := i + 1
		for j < len(format) && format[j] >= '0' && format[j] <= '9' {
			j++
		}
		if j < len(format) && format[j] == ']' && j > i+1 {
			return j + 1
		}
	}
	return i
}

// parseDirectives scans a format string and returns all fmt directives.
// It handles: %%, %s, %d, %[1]s, %[2]d, %+d, %02d, %.2f, %[1]02d, %'d, %*[2]d, etc.
func parseDirectives(format string) []directive {
	var directives []directive
	i := 0
	for i < len(format) {
		if format[i] != '%' {
			i++
			continue
		}
		start := i
		i++ // skip '%'
		if i >= len(format) {
			break
		}

		// Check for literal %%
		if format[i] == '%' {
			directives = append(directives, directive{
				start:     start,
				end:       i + 1,
				isLiteral: true,
			})
			i++
			continue
		}

		// Parse optional explicit argument index [n]
		hasIndex := false
		index := 0
		if format[i] == '[' {
			j := i + 1
			for j < len(format) && format[j] >= '0' && format[j] <= '9' {
				j++
			}
			if j < len(format) && format[j] == ']' && j > i+1 {
				n, _ := strconv.Atoi(format[i+1 : j])
				hasIndex = true
				index = n
				i = j + 1
			}
		}

		// Skip flags: #0+- space and ' (apostrophe for grouping)
		for i < len(format) {
			c := format[i]
			if c == '#' || c == '0' || c == '+' || c == '-' || c == ' ' || c == '\'' {
				i++
			} else {
				break
			}
		}

		// Skip width: * (with optional [n]) or digits
		if i < len(format) && format[i] == '*' {
			i++
			// Skip optional explicit index after * (e.g., *[2])
			i = skipOptionalIndex(format, i)
		} else {
			for i < len(format) && format[i] >= '0' && format[i] <= '9' {
				i++
			}
		}

		// Skip precision: . followed by * (with optional [n]) or digits
		if i < len(format) && format[i] == '.' {
			i++
			if i < len(format) && format[i] == '*' {
				i++
				// Skip optional explicit index after * (e.g., .*[2])
				i = skipOptionalIndex(format, i)
			} else {
				for i < len(format) && format[i] >= '0' && format[i] <= '9' {
					i++
				}
			}
		}

		// The verb: a single byte (typically a letter)
		if i < len(format) {
			i++ // consume the verb
			directives = append(directives, directive{
				start:    start,
				end:      i,
				hasIndex: hasIndex,
				index:    index,
			})
		}
	}
	return directives
}

// needsExplicitPath returns true if we need to use the explicit-index-aware path.
func needsExplicitPath(format string, args []interface{}) bool {
	// Check if any arg is a Query with argIndices
	for _, arg := range args {
		if q, ok := arg.(*Query); ok && q.argIndices != nil {
			return true
		}
	}
	// Check if format uses explicit indexing
	for _, d := range parseDirectives(format) {
		if d.hasIndex {
			return true
		}
	}
	return false
}

// Sprintf formats according to a format specifier and returns the resulting
// Query.
func Sprintf(format string, args ...interface{}) *Query {
	if needsExplicitPath(format, args) {
		return sprintfExplicit(format, args...)
	}

	// Original behavior for non-indexed format strings without explicit-index queries
	f := make([]interface{}, len(args))
	a := make([]interface{}, 0, len(args))
	for i, arg := range args {
		if q, ok := arg.(*Query); ok {
			f[i] = ignoreFormat{q.fmt}
			a = append(a, q.args...)
		} else {
			f[i] = ignoreFormat{"%s"}
			a = append(a, arg)
		}
	}
	// The format string below goes through fmt.Sprintf, which would reduce `%%` (a literal `%`
	// according to fmt format specifier semantics), but it would also go through fmt.Sprintf
	// again at Query.Query(binder) time - so we need to make sure `%%` remains as `%%` in our
	// format string. See the literal_percent_operator test.
	format = strings.ReplaceAll(format, "%%", "%%%%")
	return &Query{
		fmt:  fmt.Sprintf(format, f...),
		args: a,
	}
}

// nestedInfo tracks a nested query that has been processed for reuse.
type nestedInfo struct {
	offset     int    // offset in resultArgs where this query's args start
	fmt        string // the inlined format string
	argIndices []int  // the argIndices to use (adjusted for 1:1 if nil in original)
}

// sprintfExplicit handles format strings with explicit argument indexing like %[1]s,
// and also handles composition with nested queries that have argIndices.
func sprintfExplicit(format string, args ...interface{}) *Query {
	directives := parseDirectives(format)

	// Build the result format string and track argument indices
	var resultFmt strings.Builder
	var argIndices []int
	var resultArgs []interface{}

	// Map from original arg index to position in resultArgs (for regular args)
	argToResultPos := make(map[int]int)

	// Track nested queries by pointer for reuse when the same *Query is referenced multiple times
	nestedQueries := make(map[*Query]*nestedInfo)

	lastEnd := 0
	currentImplicitArg := 0

	for _, d := range directives {
		// Write text before this directive
		resultFmt.WriteString(format[lastEnd:d.start])
		lastEnd = d.end

		// Handle literal %%
		if d.isLiteral {
			resultFmt.WriteString("%%")
			continue
		}

		// Determine which argument this verb refers to
		var argIdx int
		if d.hasIndex {
			argIdx = d.index - 1 // Convert 1-based to 0-based
			// Per Go fmt spec: after [n], subsequent implicit verbs use n+1, n+2, etc.
			currentImplicitArg = d.index // n is 1-based, so this sets next implicit to n (0-based)
		} else {
			argIdx = currentImplicitArg
			currentImplicitArg++
		}

		if argIdx < 0 || argIdx >= len(args) {
			// Invalid index: panic with clear message (programmer error)
			// Report 1-based index to match Go's fmt convention
			panic(fmt.Sprintf("sqlf.Sprintf: argument index [%d] out of range; have %d args", argIdx+1, len(args)))
		}

		arg := args[argIdx]

		if q, ok := arg.(*Query); ok {
			// Check if we've already processed this nested query (by pointer identity)
			if info, ok := nestedQueries[q]; ok {
				// Reuse: emit the same format string and reuse the same arg indices
				resultFmt.WriteString(info.fmt)
				for _, qArgIdx := range info.argIndices {
					argIndices = append(argIndices, info.offset+qArgIdx)
				}
			} else {
				// First time seeing this nested query
				offset := len(resultArgs)
				resultFmt.WriteString(q.fmt)

				var qArgIndices []int
				if q.argIndices != nil {
					qArgIndices = q.argIndices
				} else {
					// Legacy query: generate 1:1 indices
					qArgIndices = make([]int, len(q.args))
					for i := range q.args {
						qArgIndices[i] = i
					}
				}

				for _, qArgIdx := range qArgIndices {
					argIndices = append(argIndices, offset+qArgIdx)
				}

				resultArgs = append(resultArgs, q.args...)

				// Cache for reuse by pointer identity
				nestedQueries[q] = &nestedInfo{
					offset:     offset,
					fmt:        q.fmt,
					argIndices: qArgIndices,
				}
			}
		} else {
			// Regular argument: add %s placeholder
			resultFmt.WriteString("%s")

			// Check if we've already added this argument
			if pos, ok := argToResultPos[argIdx]; ok {
				argIndices = append(argIndices, pos)
			} else {
				pos := len(resultArgs)
				argToResultPos[argIdx] = pos
				resultArgs = append(resultArgs, arg)
				argIndices = append(argIndices, pos)
			}
		}
	}

	// Write remaining text after last directive
	resultFmt.WriteString(format[lastEnd:])

	return &Query{
		fmt:        resultFmt.String(),
		args:       resultArgs,
		argIndices: argIndices,
	}
}

// Query returns a string for use in database/sql/db.Query. binder is used to
// update the format specifiers with the relevant BindVar format
func (q *Query) Query(binder BindVar) string {
	if q.argIndices != nil {
		// Explicit indexing: use argIndices to map placeholders to bind vars
		a := make([]interface{}, len(q.argIndices))
		for i, argIdx := range q.argIndices {
			a[i] = ignoreFormat{binder.BindVar(argIdx)}
		}
		return fmt.Sprintf(q.fmt, a...)
	}

	// Legacy behavior: 1:1 mapping
	a := make([]interface{}, len(q.args))
	for i := range a {
		a[i] = ignoreFormat{binder.BindVar(i)}
	}
	return fmt.Sprintf(q.fmt, a...)
}

// Args returns the args for use in database/sql/db.Query along with
// q.Query()
func (q *Query) Args() []interface{} {
	return q.args
}

// Join concatenates the elements of queries to create a single Query. The
// separator string sep is placed between elements in the resulting Query.
//
// This is commonly used to join clauses in a WHERE query. As such sep is
// usually "AND" or "OR".
func Join(queries []*Query, sep string) *Query {
	f := make([]string, 0, len(queries))
	var a []interface{}
	var argIndices []int
	hasExplicitIndices := false

	// Check if any query uses explicit indices
	for _, q := range queries {
		if q.argIndices != nil {
			hasExplicitIndices = true
			break
		}
	}

	offset := 0
	for _, q := range queries {
		f = append(f, q.fmt)

		if hasExplicitIndices {
			if q.argIndices != nil {
				for _, idx := range q.argIndices {
					argIndices = append(argIndices, offset+idx)
				}
			} else {
				// Legacy query: generate 1:1 indices
				for i := range q.args {
					argIndices = append(argIndices, offset+i)
				}
			}
		}

		a = append(a, q.args...)
		offset += len(q.args)
	}

	result := &Query{
		fmt:  strings.Join(f, " "+sep+" "),
		args: a,
	}
	if hasExplicitIndices {
		result.argIndices = argIndices
	}
	return result
}

type ignoreFormat struct{ s string }

func (e ignoreFormat) Format(f fmt.State, c rune) {
	io.WriteString(f, e.s)
}
