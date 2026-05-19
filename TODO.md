# TODO

Living punch list of known gaps and follow-ups. Add as you discover, strike when done.

## Resolver — correctness gaps (produce wrong/incomplete lineage)

- [x] **Window function `OVER` clauses are not walked.** `Function.over` is ignored in `resolve_function`, so columns inside `PARTITION BY` / `ORDER BY` of window specs (e.g. `row_number() OVER (PARTITION BY request_id ORDER BY manifestation_time)`) never get resolved and never contribute to lineage. Symptom: windowed projection columns silently lose deps. Fix lives in `src/resolve/function.rs` — walk `Function.over: Option<WindowType>`, plus `filter`, `within_group`, `null_treatment` if relevant.
- [x] **`GROUP BY` exprs aren't traversed.** `resolve_select` skips from projection to HAVING with no group-by step. Symptom: columns appearing only in GROUP BY don't resolve and don't populate `group_by_columns`. Wire a `resolve_group_by` and push an accumulator that drains into `scope.group_by_columns`.
- [ ] **SELECT-list back-references (positional + alias) in GROUP BY / HAVING / ORDER BY aren't resolved.** Two forms, same underlying fix:
    - *Positional* — `Expr::Value(1)` (e.g. `ORDER BY 1, 2`, or `GROUP BY 1` in dialects that allow it) currently no-ops. Should dereference to `selected_columns[i-1]` and reuse its deps.
    - *Alias* — `Expr::Identifier("total")` where `total` is a SELECT alias (allowed in MySQL/Postgres/SQLite/BigQuery/Snowflake for all three clauses; allowed even in standard SQL for ORDER BY). Today falls through to column lookup and errors as `ColumnNotFound`.
    Fix: in those three clauses, before resolving as a column ref, consult the active scope's `selected_columns` — by index for positional, by name match for alias — and reuse the entry's `dependencies` as the dep set.

## Resolver — incomplete / unsupported features

- [ ] `TableFactor::TableFunction`, `TableFactor::Function`, `TableFactor::UNNEST` — currently resolve their exprs but don't register as sources (TODO comments in `from.rs`).
- [ ] `TableFactor::Pivot` / `Unpivot` — only the inner table is traversed; pivot semantics are dropped.
- [ ] `TableFactor::MatchRecognize` / `XmlTable` / `SemanticView` / `JsonTable` / `OpenJsonTable` — return `UnsupportedTableFactor`.
- [ ] `Statement::Insert` / `Update` / `Delete` / `Merge` / etc. — only `Statement::Query` is supported.
- [ ] `Expr::Lambda`, `Expr::MatchAgainst`, `Expr::MemberOf`, `Expr::Dictionary`, `Expr::Map`, `Expr::Interval` — currently no-op in `resolve_expr`; revisit if any can contain column refs.
- [ ] `JoinConstraint::Using` — recognized but doesn't populate `join_columns`. `Natural` joins similar.

## Resolver — design follow-ups

- [ ] **Drop guard for `push_accumulator`/`pop_accumulator`.** Early-return on `?` between push and pop would leak a frame. Cheap to make impossible.
- [ ] **Tighten `ColumnRef` invariant.** Today `source_name: String` is a key in *some* visible scope's `sources` map, but consumers must search to find which scope. Consider adding `ScopeId` to make the resolution explicit.
- [ ] **Wire up `ColumnRefSource` enum** in `scope/mod.rs` — declared but unused. Likely the seed for typed dependencies (direct / predicate / join / group / sort).
- [ ] **`ResolutionOptions` fields are dead.** `expand_select_wildcards`, `qualify_columns`, `qualify_tables` are declared but never read. Either wire them up or remove.

## Testing

- [ ] Fill the `#[cfg(test)] mod tests` stubs in `src/resolve/column.rs` and `src/resolve/select.rs`. Start with a fake `SchemaProvider`; cover simple / qualified / ambiguous / not-found column resolution.
- [ ] Add an integration test for the `query1.sql` lineage snapshot — guards against regressions in the role-tagged dep sets.

## Downstream modules

- [ ] `src/lineage/mod.rs` is a stub. With `selected_columns[i].dependencies` + scope-level dep sets now populated, this is unblocked.
