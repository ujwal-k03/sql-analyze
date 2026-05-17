use crate::resolve::errors::ResolutionError;
use crate::resolve::{ResolutionContext, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{AccessExpr, Expr, ObjectNamePart, Subscript};

impl<'r, T: SchemaProvider> ResolutionContext<'r, T> {
    /// Recursively resolve an expression.
    pub(crate) fn resolve_expr(
        &mut self,
        expr: &mut Expr,
    ) -> Result<(), ResolutionError> {
        match expr {
            // === Direct subquery containers (create new scope) ===
            // Subqueries are not sources — their internal column references
            // propagate into every enclosing accumulator via record_column's
            // write-to-all behavior, so no explicit boundary handling is needed.
            Expr::Subquery(query) => {
                self.resolve_query(query, ScopeType::Subquery, true)?;
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.resolve_expr(expr)?;
                self.resolve_query(subquery, ScopeType::Subquery, true)?;
            }
            Expr::Exists { subquery, .. } => {
                self.resolve_query(subquery, ScopeType::Subquery, true)?;
            }

            // === Recursive cases (may contain nested subqueries) ===
            Expr::BinaryOp { left, right, .. } => {
                self.resolve_expr(left)?;
                self.resolve_expr(right)?;
            }
            Expr::UnaryOp { expr, .. } => {
                self.resolve_expr(expr)?;
            }
            Expr::Nested(expr) => {
                self.resolve_expr(expr)?;
            }
            Expr::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    self.resolve_expr(op)?;
                }
                for cond in conditions {
                    self.resolve_expr(&mut cond.condition)?;
                    self.resolve_expr(&mut cond.result)?;
                }
                if let Some(el) = else_result {
                    self.resolve_expr(el)?;
                }
            }
            Expr::Function(func) => {
                self.resolve_function(func)?;
            }
            Expr::InList { expr, list, .. } => {
                self.resolve_expr(expr)?;
                self.resolve_expr_slice(list)?;
            }
            // Same signature: single Box<Expr> — recurse on expr only
            Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::OuterJoin(expr)
            | Expr::Prior(expr) => {
                self.resolve_expr(expr)?;
            }

            // Same signature: struct with expr (recurse only on expr; other fields are non-Expr or already handled elsewhere)
            Expr::IsNormalized { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::JsonAccess { value: expr, .. }
            | Expr::Extract { expr, .. }
            | Expr::Ceil { expr, .. }
            | Expr::Floor { expr, .. }
            | Expr::Prefixed { value: expr, .. }
            | Expr::Named { expr, .. }
            | Expr::Cast { expr, .. } => {
                self.resolve_expr(expr)?;
            }

            // Same signature: two Box<Expr> (left and right + other)
            Expr::IsDistinctFrom(left, right)
            | Expr::IsNotDistinctFrom(left, right)
            | Expr::AtTimeZone { timestamp: left, time_zone: right }
            | Expr::Position { expr: left, r#in: right }
            | Expr::AnyOp { left, right, .. }
            | Expr::AllOp { left, right, .. } => {
                self.resolve_expr(left)?;
                self.resolve_expr(right)?;
            }

            // Same signature: two Box<Expr> (expr and pattern + other)
            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. }
            | Expr::RLike { expr, pattern, .. } => {
                self.resolve_expr(expr)?;
                self.resolve_expr(pattern)?;
            }

            // Unique ones
            // TODO: should Unnest have its own scope?
            Expr::InUnnest { expr, array_expr, .. } => {
                self.resolve_expr(expr)?;
                self.resolve_expr(array_expr)?;
            }
            Expr::Between { expr, low, high, .. } => {
                self.resolve_expr(expr)?;
                self.resolve_expr(low)?;
                self.resolve_expr(high)?;
            }
            Expr::Convert { expr, styles, .. } => {
                self.resolve_expr(expr)?;
                self.resolve_expr_slice(styles)?;
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                self.resolve_expr(root)?;
                for access in access_chain {
                    match access {
                        AccessExpr::Dot(expr) => self.resolve_expr(expr)?,
                        AccessExpr::Subscript(sub) => match sub {
                            Subscript::Index { index } => self.resolve_expr(index)?,
                            Subscript::Slice {
                                lower_bound,
                                upper_bound,
                                stride,
                            } => {
                                for opt in [lower_bound, upper_bound, stride] {
                                    if let Some(e) = opt {
                                        self.resolve_expr(e)?
                                    }
                                }
                            }
                        },
                    }
                }
            }
            Expr::Substring { expr, substring_from, substring_for, .. } => {
                self.resolve_expr(expr)?;
                for opt in [substring_from.as_mut(), substring_for.as_mut()] {
                    if let Some(e) = opt {
                        self.resolve_expr(e)?;
                    }
                }
            }
            Expr::Trim { expr, trim_what, trim_characters, .. } => {
                self.resolve_expr(expr)?;
                if let Some(e) = trim_what {
                    self.resolve_expr(e)?;
                }
                if let Some(list) = trim_characters {
                    self.resolve_expr_slice(list)?;
                }
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                self.resolve_expr(expr)?;
                self.resolve_expr(overlay_what)?;
                self.resolve_expr(overlay_from)?;
                if let Some(e) = overlay_for {
                    self.resolve_expr(e)?;
                }
            }
            Expr::Tuple(exprs) => {
                self.resolve_expr_slice(exprs)?;
            }
            Expr::GroupingSets(sets) => {
                for list in sets {
                    self.resolve_expr_slice(list)?;
                }
            }
            Expr::Cube(sets)
            | Expr::Rollup(sets) => {
                for list in sets {
                    self.resolve_expr_slice(list)?;
                }
            }
            Expr::Struct { values, .. } => {
                self.resolve_expr_slice(values)?;
            }
            Expr::Array(arr) => {
                self.resolve_expr_slice(&mut arr.elem)?;
            }
            // No nested Expr (or handled elsewhere)
            Expr::Identifier(ident) => {
                *expr = Expr::CompoundIdentifier(self.resolve_col(ident, &[])?)
            }
            Expr::CompoundIdentifier(ident_vec) => {
                let col_ident = &ident_vec[ident_vec.len() - 1].clone();
                let source_name: Vec<ObjectNamePart> = ident_vec[..ident_vec.len() - 1]
                    .iter()
                    .map(|x| ObjectNamePart::Identifier(x.clone()))
                    .collect();
                *expr = Expr::CompoundIdentifier(self.resolve_col(col_ident, &source_name)?);
            }
            | Expr::Value(_) // No recurse
            | Expr::Wildcard(_) // Should I expand this?
            | Expr::QualifiedWildcard(..) // Should I expand this?
            | Expr::TypedString(_) // No recurse
            | Expr::Interval(_) // Should I resolve this?
            | Expr::MatchAgainst { .. } // Should I resolve this?
            | Expr::Lambda(_) // Should I resolve this?
            | Expr::MemberOf(_) // Should I resolve this?
            | Expr::Dictionary(_) // Should I resolve this?
            | Expr::Map(_) => {} // Should I resolve this?
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn resolve_expr_slice(
        &mut self,
        exprs: &mut [Expr],
    ) -> Result<(), ResolutionError>{
        for e in exprs.as_mut() {
            self.resolve_expr(e)?
        }
        Ok(())
    }

}
