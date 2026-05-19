use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::sources::{ResolvedSource, ScopeSource, Source, TableSource};
use crate::resolve::{ResolutionContext, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{Ident, JoinConstraint, JoinOperator, ObjectNamePart, TableAlias, TableFactor, TableWithJoins};

impl<'r, T: SchemaProvider> ResolutionContext<'r, T> {
    pub(crate) fn resolve_table_with_joins(
        &mut self,
        table_with_join: &mut TableWithJoins,
    ) -> Result<(), ResolutionError> {
        self.resolve_table_factor(&mut table_with_join.relation)?;

        for join in &mut table_with_join.joins {
            self.resolve_table_factor(&mut join.relation)?;

            match &mut join.join_operator {
                JoinOperator::Join(constraint)
                | JoinOperator::Inner(constraint)
                | JoinOperator::Left(constraint)
                | JoinOperator::LeftOuter(constraint)
                | JoinOperator::Right(constraint)
                | JoinOperator::RightOuter(constraint)
                | JoinOperator::FullOuter(constraint)
                | JoinOperator::CrossJoin(constraint)
                | JoinOperator::Semi(constraint)
                | JoinOperator::LeftSemi(constraint)
                | JoinOperator::RightSemi(constraint)
                | JoinOperator::Anti(constraint)
                | JoinOperator::LeftAnti(constraint)
                | JoinOperator::RightAnti(constraint)
                | JoinOperator::StraightJoin(constraint) => {
                    self.resolve_join_constraint(constraint)?;
                }

                JoinOperator::CrossApply
                | JoinOperator::OuterApply => {}

                JoinOperator::AsOf { match_condition, constraint } => {
                    // The match_condition for the temporal AS OF join needs to be
                    // accumulated separately
                    self.push_accumulator();
                    self.resolve_expr(match_condition)?;
                    let deps = self.pop_accumulator();
                    self.active_scope().join_columns.extend(deps);

                    // Proceed as normal
                    self.resolve_join_constraint(constraint)?;
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn resolve_join_constraint(
        &mut self,
        constraint: &mut JoinConstraint,
    ) -> Result<(), ResolutionError> {
        match constraint {
            JoinConstraint::On(expr) => {
                self.push_accumulator();
                self.resolve_expr(expr)?;
                let deps = self.pop_accumulator();
                self.active_scope().join_columns.extend(deps);
                Ok(())
            }
            // TODO: Need to add support for Using/Natural
            JoinConstraint::Using(_)
            | JoinConstraint::Natural
            | JoinConstraint::None => {Ok(())}
        }
    }

    fn resolve_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> Result<(), ResolutionError> {
        // FROM ...
        // ├─ LATERAL func(...) → TableFactor::Function { lateral: true }
        // ├─ LATERAL (...) → TableFactor::Derived { lateral: true }
        // └─ name(...) → TableFactor::Table { args: Some(...) }
        //     └─ name → TableFactor::Table { args: None }
        match table_factor {
            TableFactor::Table{name, alias, args, ..} => {
                if args.is_some() {
                    return Err(ResolutionError::UnsupportedTableWithArguments(table_factor.to_string()))
                }

                // The table could either be CTE or a base table, we need to check the CTE case first.
                // ------ CTE CASE ---------
                // SQL:2003 BNF (ISO/IEC 9075-2:2003) defines:
                //   <query name> ::= <identifier>
                // So a CTE name is structurally a single identifier, i.e - x.y.z can't be a CTE.
                //
                // Unlike column resolution, the CTE walk is NOT gated by `allow_lateral`.
                // CTEs are lexically scoped: any enclosing WITH is visible regardless of
                // whether the current scope was entered laterally. On miss, we fall through
                // to the base-table lookup - a bare name shadows a base table only if a
                // matching CTE exists.
                if name.0.len() == 1 && let ObjectNamePart::Identifier(ident) = &name.0[0] {
                    let cte_name = ident.value.clone();

                    for _scope_id in self.visible_scopes.iter().rev() {
                        let scope = &self.scopes[*_scope_id];
                        if let Some(&cte_scope_id) = scope.ctes.get(&cte_name) {
                            let scope_source = ScopeSource::from_scope(
                                &self.scopes[cte_scope_id],
                                &cte_name,
                                alias,
                            )?;

                            self.register_aliased_source(ResolvedSource::Cte(scope_source), alias);
                            return Ok(());
                        }
                    }
                }

                // ------ BASE TABLE CASE -----
                // Since the schema provider requires a proper name (Vec<String>), we need to
                // process the [ObjectNamePart], and reject any unsupported parts (see: functions)
                let mut table_ident: Vec<String> = Vec::new();
                for name_part in name.0.iter() {
                    if let Some(part_ident) = name_part.as_ident() {
                        table_ident.push(part_ident.value.clone());
                    } else {
                        return Err(ResolutionError::UnsupportedObjectNamePart(name_part.clone()))
                    }
                }

                if let Some(schema) = self.resolver.schema_provider.get_schema(&table_ident) {
                    let source = TableSource::from_schema(table_ident, schema, alias)?;
                    self.register_aliased_source(ResolvedSource::Table(source), alias);
                } else {
                    return Err(ResolutionError::TableNotFound(name.to_string()))
                }

            }
            TableFactor::Derived{lateral, subquery, alias} => {
                // Derived Tables. These are paranthesised subqueries - subject to resolution.
                let dt_scope_id = self.resolve_query(
                    subquery,
                    ScopeType::DerivedTable,
                    *lateral,
                )?;

                let scope_source = ScopeSource::from_scope(
                    &self.scopes[dt_scope_id],
                    &format!("__derived{}", dt_scope_id),
                    alias
                )?;

                self.register_aliased_source(ResolvedSource::DerivedTable(scope_source), alias);
            }
            TableFactor::NestedJoin { table_with_joins, ..} => {
                self.resolve_table_with_joins(table_with_joins)?;
            }
            // TODO: each of these can produce a source whose columns are referenceable
            // downstream (table functions, UNNEST output, pivoted/unpivoted shapes,
            // MATCH_RECOGNIZE measures, XML/JSON table projections). Until that's
            // implemented, fail loudly rather than silently registering no source -
            // a silent miss leaks through as a confusing ColumnNotFound later.
            TableFactor::TableFunction {..}
            | TableFactor::Function {..}
            | TableFactor::UNNEST {..}
            | TableFactor::Pivot {..}
            | TableFactor::Unpivot {..}
            | TableFactor::MatchRecognize {..}
            | TableFactor::XmlTable {..}
            | TableFactor::SemanticView {..}
            | TableFactor::JsonTable {..}
            | TableFactor::OpenJsonTable {..} => {
                return Err(ResolutionError::UnsupportedTableFactor(table_factor.to_string()))
            }
        }

        Ok(())
    }

    /// Registers a source for the current scope under a unique internal key,
    /// and rewrites the AST alias to match that key.
    ///
    /// Two namespaces are kept deliberately out of sync:
    ///   - the map key / synthesized alias is deconflicted (`users`, `users_`, ...)
    ///     so `*` expansion can emit unambiguous, re-serializable SQL like
    ///     `SELECT users.id, users_.id FROM users JOIN users`.
    ///   - `Source::match_name` still compares against the *original* identifier,
    ///     so user-facing ambiguity (e.g. `SELECT users.id FROM users JOIN users`)
    ///     still errors as expected.
    fn register_aliased_source(
        &mut self,
        source: ResolvedSource,
        alias: &mut Option<TableAlias>
    ) {
        let correlation_name = source.get_correlation_name();

        // Append `_` until the key is free. Keeps `sources` injective so star
        // expansion and internal lookups can address each source uniquely.
        let mut aliased_name = correlation_name;
        loop {
            if !self.active_scope().sources.contains_key(&aliased_name) {
                break;
            } else {
                aliased_name = aliased_name + "_";
            }
        }

        self.active_scope().sources.insert(aliased_name.clone(), source);

        // Sync the AST alias to the deconflicted key. Synthesize one if absent,
        // so later passes (esp. wildcard expansion) can emit qualified refs.
        if let Some(table_alias) = alias {
            table_alias.name.value = aliased_name;
        } else {
            *alias = Some(TableAlias{
                explicit: true,
                name: Ident::new(aliased_name),
                columns: vec![],
            })
        }
    }
}