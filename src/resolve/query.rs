use crate::resolve::errors::ResolutionError;
use crate::resolve::{Resolver, ScopeId, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{Cte, OrderBy, OrderByKind, Query, SetExpr};

impl<'a, T: SchemaProvider> Resolver<T> {
    pub(crate) fn resolve_query(
        &mut self,
        query: &mut Query,
        scope_type: ScopeType,
        allow_lateral: bool,
    ) -> Result<ScopeId, ResolutionError> {
        self.branch_scope(scope_type, allow_lateral);

        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                self.register_cte(cte)?;
            }
        }

        // Resolving the body of the query has to be one of the first things we do.
        // This involves things like populating the sources in the current scope.
        self.resolve_set_expr(&mut query.body, false)?;

        // ORDER BY
        if let Some(order_by) = &mut query.order_by {
            self.resolve_order_by(order_by)?;
        }

        // Then pop the scope itself
        Ok(self.exit_scope())
    }

    fn resolve_set_expr(
        &mut self,
        set_expr: &mut SetExpr,
        is_setop_branch: bool,
    ) -> Result<(), ResolutionError> {
        match set_expr {
            SetExpr::Select(select) => {
                // If the current SetExpr is a branch of a SetOperation
                if is_setop_branch {
                    self.resolve_select(select, true)?;
                } else {
                    self.resolve_select(select, false)?;
                }
            }
            SetExpr::SetOperation {left, right, ..} => {
                self.resolve_set_expr(left, true)?;
                self.resolve_set_expr(right, true)?;
            }
            SetExpr::Query(subquery) => {
                // If the Query is a branch of a SetOperation, the scope type should be SetOpBranch.
                // This exists for cleanliness and probably won't impact analysis that much.
                let scope_type = if is_setop_branch { ScopeType::SetOpBranch } else { ScopeType::Subquery };
                self.resolve_query(
                    subquery,
                    scope_type,
                    false,
                )?;
            }
            // Not using _ so that the compiler will warn us in case
            // of missed branches
            SetExpr::Values(_)
            | SetExpr::Insert(_)
            | SetExpr::Update(_)
            | SetExpr::Delete(_)
            | SetExpr::Merge(_)
            | SetExpr::Table(_) => {}
        }

        Ok(())
    }

    fn register_cte(
        &mut self,
        cte: &mut Cte,
    ) -> Result<(), ResolutionError> {
        let cte_scope = self.resolve_query(
            &mut cte.query,
            ScopeType::Cte,
            false,
        )?;

        // Handle duplicate cte
        if self.active_scope().ctes.contains_key(&cte.alias.name.value) {
            return Err(ResolutionError::DuplicateCte(cte.alias.name.value.clone()));
        }

        // Handle Cte column aliases
        if cte.alias.columns.len() > 0 {
            if cte.alias.columns.len() != self.scopes[cte_scope].outer_columns.len() {
                return Err(ResolutionError::AliasLengthMismatch(cte.alias.name.value.clone()));
            } else {
                for (i, col) in cte.alias.columns.iter().enumerate() {
                    self.scopes[cte_scope].outer_columns[i].name.replace_range(.., &col.name.value);
                }
            }
        }

        self.active_scope().ctes.insert(cte.alias.name.value.clone(), cte_scope);
        Ok(())
    }

    fn resolve_order_by(
        &mut self,
        order_by: &mut OrderBy,
    ) -> Result<(), ResolutionError> {
        match &mut order_by.kind {
            OrderByKind::All(_) => {}
            OrderByKind::Expressions(order_by_exprs) => {
                for order_by_expr in order_by_exprs {
                    self.resolve_expr(&mut order_by_expr.expr, &mut None)?;

                    if let Some(with_fill) = &mut order_by_expr.with_fill {
                        if let Some(from) = &mut with_fill.from {
                            self.resolve_expr(from, &mut None)?;
                        }
                        if let Some(to) = &mut with_fill.to {
                            self.resolve_expr(to, &mut None)?;
                        }
                        if let Some(step) = &mut with_fill.step {
                            self.resolve_expr(step, &mut None)?;
                        }
                    }
                }
            }
        }

        if let Some(interpolate) = &mut order_by.interpolate {
            if let Some(exprs) = &mut interpolate.exprs {
                for expr in exprs {
                    self.resolve_col(&mut vec![expr.column.clone()], &mut None)?;
                    if let Some(expr) = &mut expr.expr {
                        self.resolve_expr(expr, &mut None)?;
                    }
                }
            }
        }

        Ok(())
    }
}