package sqlf_test

import (
	"reflect"
	"testing"

	"github.com/keegancsmith/sqlf"
)

func TestSprintf(t *testing.T) {
	cases := map[string]struct {
		Fmt      string
		FmtArgs  []interface{}
		Want     string
		WantArgs []interface{}
	}{
		"simple_substitute": {
			"SELECT * FROM test_table WHERE a = %s AND b = %d",
			[]interface{}{"foo", 1},
			"SELECT * FROM test_table WHERE a = $1 AND b = $2",
			[]interface{}{"foo", 1},
		},

		"simple_embedded": {
			"SELECT * FROM test_table WHERE a = (%s)",
			[]interface{}{sqlf.Sprintf("SELECT b FROM b_table WHERE x = %d", 1)},
			"SELECT * FROM test_table WHERE a = (SELECT b FROM b_table WHERE x = $1)",
			[]interface{}{1},
		},

		"embedded": {
			"SELECT * FROM test_table WHERE a = %s AND c = (%s) AND d = %s",
			[]interface{}{"foo", sqlf.Sprintf("SELECT b FROM b_table WHERE x = %d", 1), "bar"},
			"SELECT * FROM test_table WHERE a = $1 AND c = (SELECT b FROM b_table WHERE x = $2) AND d = $3",
			[]interface{}{"foo", 1, "bar"},
		},

		"embedded_embedded": {
			"SELECT * FROM test_table WHERE a = %s AND c = (%s) AND d = %s",
			[]interface{}{
				"foo",
				sqlf.Sprintf("SELECT b FROM b_table WHERE x = %d AND y = (%s)", 1, sqlf.Sprintf("SELECT %s", "baz")),
				"bar",
			},
			"SELECT * FROM test_table WHERE a = $1 AND c = (SELECT b FROM b_table WHERE x = $2 AND y = (SELECT $3)) AND d = $4",
			[]interface{}{"foo", 1, "baz", "bar"},
		},

		"literal_percent_operator": {
			"SELECT * FROM test_table WHERE a <<%% %s AND b = %d",
			[]interface{}{"foo", 1},
			"SELECT * FROM test_table WHERE a <<% $1 AND b = $2",
			[]interface{}{"foo", 1},
		},

		"explicit_index_single_reuse": {
			"UPDATE t SET a = %[1]s, b = %[1]s WHERE c = %[1]s",
			[]interface{}{"val"},
			"UPDATE t SET a = $1, b = $1 WHERE c = $1",
			[]interface{}{"val"},
		},

		"explicit_index_multiple_args": {
			"SELECT * FROM t WHERE a = %[1]s AND b = %[2]s AND c = %[1]s",
			[]interface{}{"foo", "bar"},
			"SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1",
			[]interface{}{"foo", "bar"},
		},

		"explicit_index_with_percent": {
			"SELECT * FROM t WHERE a %% %[1]s AND b = %[1]s",
			[]interface{}{"val"},
			"SELECT * FROM t WHERE a % $1 AND b = $1",
			[]interface{}{"val"},
		},

		"explicit_index_realistic": {
			`UPDATE changesets SET batch_change_ids = batch_change_ids - %[1]s::text, updated_at = NOW(), detached_at = CASE WHEN batch_change_ids - %[1]s::text = '{}'::jsonb THEN NOW() ELSE detached_at END WHERE batch_change_ids ? %[1]s::text`,
			[]interface{}{"123"},
			`UPDATE changesets SET batch_change_ids = batch_change_ids - $1::text, updated_at = NOW(), detached_at = CASE WHEN batch_change_ids - $1::text = '{}'::jsonb THEN NOW() ELSE detached_at END WHERE batch_change_ids ? $1::text`,
			[]interface{}{"123"},
		},

		"explicit_and_implicit_mixed": {
			`UPDATE t SET a = %[1]s, b = %[1]s WHERE c = %s`,
			[]interface{}{"id", "name"},
			`UPDATE t SET a = $1, b = $1 WHERE c = $2`,
			[]interface{}{"id", "name"},
		},

		"explicit_then_implicit_multiple": {
			`SELECT * FROM t WHERE a = %[1]s AND b = %s AND c = %s`,
			[]interface{}{"x", "y", "z"},
			`SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3`,
			[]interface{}{"x", "y", "z"},
		},

		// Tests for flags/width/precision parsing
		"explicit_with_flags": {
			"SELECT * FROM t WHERE a = %[1]02d OR b = %[1]02d",
			[]interface{}{42},
			"SELECT * FROM t WHERE a = $1 OR b = $1",
			[]interface{}{42},
		},

		"explicit_with_precision": {
			"SELECT * FROM t WHERE a = %[1].2f OR b = %[1].2f",
			[]interface{}{3.14},
			"SELECT * FROM t WHERE a = $1 OR b = $1",
			[]interface{}{3.14},
		},

		"explicit_with_width_and_precision": {
			"SELECT * FROM t WHERE a = %[1]10.2f",
			[]interface{}{3.14},
			"SELECT * FROM t WHERE a = $1",
			[]interface{}{3.14},
		},

		// Tests for nested query composition
		"explicit_nested_into_implicit_outer": {
			"WHERE a = %s AND b = %s",
			[]interface{}{sqlf.Sprintf("(%[1]s OR %[1]s)", "x"), "y"},
			"WHERE a = ($1 OR $1) AND b = $2",
			[]interface{}{"x", "y"},
		},

		"explicit_nested_into_explicit_outer": {
			"WHERE a = %[1]s AND b = %[2]s AND c = %[1]s",
			[]interface{}{sqlf.Sprintf("(%[1]s OR %[1]s)", "x"), "y"},
			"WHERE a = ($1 OR $1) AND b = $2 AND c = ($1 OR $1)",
			[]interface{}{"x", "y"},
		},

		"implicit_nested_into_explicit_outer": {
			"WHERE a = %[1]s AND b = %[1]s",
			[]interface{}{sqlf.Sprintf("(%s OR %s)", "x", "y")},
			"WHERE a = ($1 OR $2) AND b = ($1 OR $2)",
			[]interface{}{"x", "y"},
		},

		// Out-of-order explicit indexing - verifies args are compacted in placeholder order
		"explicit_out_of_order": {
			"a = %[2]s AND b = %[1]s",
			[]interface{}{"first", "second"},
			"a = $1 AND b = $2",
			[]interface{}{"second", "first"},
		},

		"explicit_out_of_order_with_reuse": {
			"a = %[2]s AND b = %[1]s AND c = %[2]s",
			[]interface{}{"first", "second"},
			"a = $1 AND b = $2 AND c = $1",
			[]interface{}{"second", "first"},
		},

		// Scanner robustness: apostrophe flag for grouping
		"explicit_with_apostrophe_flag": {
			"SELECT %[1]'d, %[1]'d",
			[]interface{}{1000},
			"SELECT $1, $1",
			[]interface{}{1000},
		},

		// Scanner robustness: * width with explicit index - scanner should not break
		// Note: %*[2]d means width comes from arg[2], so we just verify parsing doesn't break
		"star_width_with_index": {
			"SELECT %[1]*[2]d",
			[]interface{}{42, 10},
			"SELECT $1",
			[]interface{}{42},
		},

		// Same *Query passed in multiple positions via explicit indexing (should reuse by pointer)
		"same_query_multiple_positions_explicit": {
			"a = %[1]s AND b = %[1]s",
			func() []interface{} {
				q := sqlf.Sprintf("(%s)", "x")
				return []interface{}{q}
			}(),
			"a = ($1) AND b = ($1)",
			[]interface{}{"x"},
		},
	}
	for tn, tc := range cases {
		q := sqlf.Sprintf(tc.Fmt, tc.FmtArgs...)
		if got := q.Query(sqlf.PostgresBindVar); got != tc.Want {
			t.Errorf("%s: expected query: %q, got: %q", tn, tc.Want, got)
		}
		if got := q.Args(); !reflect.DeepEqual(got, tc.WantArgs) {
			t.Errorf("%s: expected args: %q, got: %q", tn, tc.WantArgs, got)
		}
	}
}

func TestJoinWithExplicitIndices(t *testing.T) {
	cases := map[string]struct {
		Queries  []*sqlf.Query
		Sep      string
		Want     string
		WantArgs []interface{}
	}{
		"join_explicit_queries": {
			[]*sqlf.Query{
				sqlf.Sprintf("a = %[1]s OR a = %[1]s", "x"),
				sqlf.Sprintf("b = %s", "y"),
			},
			"AND",
			"a = $1 OR a = $1 AND b = $2",
			[]interface{}{"x", "y"},
		},

		"join_mixed_explicit_implicit": {
			[]*sqlf.Query{
				sqlf.Sprintf("a = %[1]s OR a = %[1]s", "x"),
				sqlf.Sprintf("b = %s AND c = %s", "y", "z"),
			},
			"AND",
			"a = $1 OR a = $1 AND b = $2 AND c = $3",
			[]interface{}{"x", "y", "z"},
		},

		"join_all_implicit": {
			[]*sqlf.Query{
				sqlf.Sprintf("a = %s", "x"),
				sqlf.Sprintf("b = %s", "y"),
			},
			"OR",
			"a = $1 OR b = $2",
			[]interface{}{"x", "y"},
		},
	}

	for tn, tc := range cases {
		q := sqlf.Join(tc.Queries, tc.Sep)
		if got := q.Query(sqlf.PostgresBindVar); got != tc.Want {
			t.Errorf("%s: expected query: %q, got: %q", tn, tc.Want, got)
		}
		if got := q.Args(); !reflect.DeepEqual(got, tc.WantArgs) {
			t.Errorf("%s: expected args: %v, got: %v", tn, tc.WantArgs, got)
		}
	}
}

func BenchmarkSprintf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = sqlf.Sprintf("SELECT * FROM test_table WHERE a = %s AND b = %d", "foo", 1).Query(sqlf.PostgresBindVar)
	}
}
