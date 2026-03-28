use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::sources::{ResolvedSource, ScopeSource, Source, TableSource};
use crate::resolve::{Resolver, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{Ident, JoinConstraint, JoinOperator, ObjectNamePart, TableAlias, TableFactor, TableWithJoins};

impl<'a, T: SchemaProvider> Resolver<T> {
    pub(crate) fn resolve_table_with_joins(
        &mut self,
        table_with_join: &mut TableWithJoins,
    ) -> Result<(), ResolutionError> {
        // First resolve the first table
        // println!("Checking table factor: {}", table_with_join.relation.to_string());
        self.resolve_table_factor(&mut table_with_join.relation)?;

        // Iterate over the joins
        for join in &mut table_with_join.joins {
            // println!("Checking Table factor: {}", join.relation.to_string());
            self.resolve_table_factor(&mut join.relation)?;

            // Do we need to resolve the join constraint?
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
                    self.resolve_expr(match_condition, &mut None)?;
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
                self.resolve_expr(expr, &mut None)
            }
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
                // Throw a warning if the table has arguments. A table with arguments
                // functions more like a UDTF. We should handle it differently.
                // TODO: Do the above
                if let Some(_) = args {
                    println!("Warning: Table with arguments found: {:?}", table_factor);
                    return Err(ResolutionError::UnsupportedTableWithArguments(table_factor.to_string()))
                }

                // The table could either be CTE or a base table, we need to check the CTE case first.
                // ------ CTE CASE ---------
                if name.0.len() == 1 && let ObjectNamePart::Identifier(ident) = &name.0[0] {
                    let cte_name = ident.value.clone();
                    // println!("Searching for cte {}", cte_name);

                    for _scope_id in self.visible_scopes.iter().rev() {
                        let scope = &self.scopes[*_scope_id];
                        // println!("Scope: {}, ctes: {:#?}", self.active_scope, self.scopes[*_scope_id].ctes);

                        if let Some(&cte_scope_id) = scope.ctes.get(&cte_name) {
                            // println!("Found cte as {}", cte_name);
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
                let mut table_ident: Vec<String> = Vec::new();
                for name_part in name.0.iter() {
                    if let Some(part_ident) = name_part.as_ident() {
                        table_ident.push(part_ident.value.clone());
                    } else {
                        return Err(ResolutionError::UnsupportedObjectNamePart(name_part.clone()))
                    }
                }

                if let Some(schema) = self.schema_provider.get_schema(&table_ident) {
                    let source = TableSource::from_schema(table_ident, schema, alias)?;

                    self.register_aliased_source(ResolvedSource::Table(source), alias);
                } else {
                    match &name.0[0] {
                        ObjectNamePart::Identifier(ident) => println!("{:?}, {}", ident.span, self.active_scope),
                        _ => {}
                    }
                    return Err(ResolutionError::TableNotFound(name.to_string()))
                }

            }
            TableFactor::Derived{lateral, subquery, alias, ..} => {
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
            TableFactor::TableFunction { expr, ..} => {
                // TODO: CAN ALSO BE A SOURCE
                self.resolve_expr(expr, &mut None)?;
            }
            TableFactor::Function {..} => {
                // TODO: Later
                // TODO: CAN ALSO BE A SOURCE
            }
            TableFactor::UNNEST { array_exprs, ..} => {
                // TODO: CAN ALSO BE A SOURCE
                self.resolve_expr_slice(array_exprs, &mut None)?;
            }
            TableFactor::NestedJoin { table_with_joins, ..} => {
                self.resolve_table_with_joins(table_with_joins)?;
            }
            TableFactor::Pivot { table, ..} => {
                // For now, only traversing the table factor for the pivot
                // TODO: Stretch this to the whole pivot operation
                self.resolve_table_factor(table)?;
            }
            TableFactor::Unpivot { table, ..} => {
                // For now, only traversing the table factor for the unpivot
                // TODO: Stretch this to the whole unpivot operation
                self.resolve_table_factor(table)?;
            }
            TableFactor::MatchRecognize{..}
            | TableFactor::XmlTable{..}
            | TableFactor::SemanticView{..}
            | TableFactor::JsonTable{..}
            | TableFactor::OpenJsonTable{..} => {
                // TODO: Later
                // Throw a warning to the user
                println!("Warning: Table factor not supported for scope traversal: {:?}", table_factor);
                return Err(ResolutionError::UnsupportedTableFactor(table_factor.to_string()))
            }
        }

        Ok(())
    }

    /// Registers a source for the current scope
    fn register_aliased_source(
        &mut self,
        source: ResolvedSource,
        alias: &mut Option<TableAlias>
    ) {
        let correlation_name = source.get_correlation_name();

        // Generate a new aliased name that is guaranteed to be available
        let mut aliased_name = correlation_name;
        loop {
            if !self.active_scope().sources.contains_key(&aliased_name) {
                break;
            } else {
                aliased_name = aliased_name + "_";
            }
        }

        self.active_scope().sources.insert(aliased_name.clone(), source);

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