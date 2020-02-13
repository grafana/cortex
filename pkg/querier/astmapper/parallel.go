package astmapper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

var summableAggregates = map[promql.ItemType]struct{}{
	promql.SUM:     {},
	promql.MIN:     {},
	promql.MAX:     {},
	promql.TOPK:    {},
	promql.BOTTOMK: {},
	promql.COUNT:   {},
}

var nonParallelFuncs = []string{
	"histogram_quantile",
	"quantile_over_time",
	"absent",
}

// CanParallel tests if a subtree is parallelizable.
// A subtree is parallelizable if all of its components are parallelizable.
func CanParallel(node promql.Node) bool {
	switch n := node.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return true

	case promql.Expressions:
		for _, e := range n {
			if !CanParallel(e) {
				return false
			}
		}
		return true

	case *promql.AggregateExpr:
		_, ok := summableAggregates[n.Op]
		return ok && CanParallel(n.Expr)

	case *promql.BinaryExpr:
		// since binary exprs use each side for merging, they cannot be parallelized
		return false

	case *promql.Call:
		if n.Func == nil {
			return false
		}
		if !ParallelFunc(*n.Func) {
			return false
		}

		for _, e := range n.Args {
			if !CanParallel(e) {
				return false
			}
		}
		return true

	case *promql.SubqueryExpr:
		return CanParallel(n.Expr)

	case *promql.ParenExpr:
		return CanParallel(n.Expr)

	case *promql.UnaryExpr:
		// Since these are only currently supported for Scalars, should be parallel-compatible
		return true

	case *promql.EvalStmt:
		return CanParallel(n.Expr)

	case *promql.MatrixSelector, *promql.NumberLiteral, *promql.StringLiteral, *promql.VectorSelector:
		return true

	default:
		panic(errors.Errorf("CanParallel: unhandled node type %T", node))
	}

}

// ParallelFunc ensures that a promql function can be part of a parallel query.
func ParallelFunc(f promql.Function) bool {

	for _, v := range nonParallelFuncs {
		if v == f.Name {
			return false
		}
	}
	return true
}