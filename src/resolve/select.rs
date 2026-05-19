use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::{ColumnRef, SelectedColumn};
use crate::resolve::{ResolutionContext, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{Expr, GroupByExpr, GroupByWithModifier, Ident, Select, SelectItem, SelectItemQualifiedWildcardKind};
use std::collections::HashSet;

impl<'r, T: SchemaProvider> ResolutionContext<'r, T> {
    pub(crate) fn resolve_select(
        &mut self,
        select: &mut Select,
        create_scope: bool,
    ) -> Result<(), ResolutionError> {
        if create_scope {
            self.branch_scope(ScopeType::SetOpBranch, false);
        }

        // FROM, These should be the scope's sources, and thus, need to resolved first
        for table_with_joins in &mut select.from {
            self.resolve_table_with_joins(table_with_joins)?;
        }

        // WHERE
        if let Some(selection) = &mut select.selection {
            self.push_accumulator();
            self.resolve_expr(selection)?;
            let deps = self.pop_accumulator();
            self.active_scope().filter_columns.extend(deps);
        }

        // SELECT items
        let mut resolved_items: Vec<SelectItem> = Vec::new();
        for item in &mut select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let outer_name = match expr {
                        Expr::Identifier(ident) => {ident.value.clone()}
                        Expr::CompoundIdentifier(ident) => {ident.last().unwrap().value.clone()}
                        _ => {expr.to_string()}
                    };

                    self.push_accumulator();
                    self.resolve_expr(expr)?;
                    let dependencies = self.pop_accumulator();

                    self.active_scope().selected_columns.push(SelectedColumn{
                        name: outer_name,
                        dependencies,
                    });

                    resolved_items.push(SelectItem::UnnamedExpr(expr.clone()));
                }
                SelectItem::ExprWithAlias { expr, alias} => {
                    let outer_name = alias.value.clone();

                    self.push_accumulator();
                    self.resolve_expr(expr)?;
                    let dependencies = self.pop_accumulator();

                    self.active_scope().selected_columns.push(SelectedColumn{
                        name: outer_name.clone(),
                        dependencies,
                    });

                    resolved_items.push(SelectItem::ExprWithAlias { expr: expr.clone(), alias: alias.clone() });
                }
                SelectItem::QualifiedWildcard(item, options) => {
                    match item {
                        SelectItemQualifiedWildcardKind::ObjectName(obj_name) => {

                            let expanded_columns = self.resolve_wildcard(Some(obj_name), Some(options))?;

                            for column in expanded_columns {
                                let mut dependencies: HashSet<ColumnRef> = HashSet::with_capacity(1);
                                dependencies.insert(ColumnRef {
                                    source_name: column.0.clone(),
                                    name: column.1.clone(),
                                });

                                self.active_scope().selected_columns.push(SelectedColumn{
                                    name: column.1.clone(),
                                    dependencies,
                                });

                                resolved_items.push(
                                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(
                                        vec![Ident::new(column.0), Ident::new(column.1)]
                                    ))
                                )
                            }
                        }
                        SelectItemQualifiedWildcardKind::Expr(_) => {return Err(ResolutionError::UnsupportedWildcardType(
                            format!("QualifiedWildcardKind::Expr: {:?}", item)
                        ))}
                    }
                }
                SelectItem::Wildcard(options) => {
                    let expanded_columns = self.resolve_wildcard(None, Some(options))?;

                    for column in expanded_columns {
                        let mut dependencies: HashSet<ColumnRef> = HashSet::with_capacity(1);
                        dependencies.insert(ColumnRef {
                            source_name: column.0.clone(),
                            name: column.1.clone(),
                        });

                        self.active_scope().selected_columns.push(SelectedColumn{
                            name: column.1.clone(),
                            dependencies,
                        });

                        resolved_items.push(
                            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(
                                vec![Ident::new(column.0), Ident::new(column.1)]
                            ))
                        )
                    }
                }
            }
        }
        select.projection = resolved_items;

        // GROUP BY
        self.resolve_group_by(&mut select.group_by)?;

        // HAVING, post-aggregation filter, classified alongside WHERE
        if let Some(having) = &mut select.having {
            self.push_accumulator();
            self.resolve_expr(having)?;
            let deps = self.pop_accumulator();
            self.active_scope().filter_columns.extend(deps);
        }
        // LATERAL VIEWs
        for lateral in &mut select.lateral_views {
            self.resolve_expr(&mut lateral.lateral_view)?;
        }

        if create_scope {
            self.exit_scope();
        }

        // TODO: Choosing to skip the rest for now. Maybe revisit later. :0
        Ok(())
    }

    fn resolve_group_by(
        &mut self,
        group_by: &mut GroupByExpr,
    ) -> Result<(), ResolutionError> {
        match group_by {
            GroupByExpr::All(modifiers) => {
                self.resolve_group_by_modifiers(modifiers)?;
            }
            GroupByExpr::Expressions(exprs, modifiers) => {
                for expr in exprs {
                    self.push_accumulator();
                    self.resolve_expr(expr)?;
                    let deps = self.pop_accumulator();
                    self.active_scope().group_by_columns.extend(deps);
                }
                self.resolve_group_by_modifiers(modifiers)?;
            }
        }
        Ok(())
    }

    fn resolve_group_by_modifiers(
        &mut self,
        modifiers: &mut [GroupByWithModifier],
    ) -> Result<(), ResolutionError> {
        for modifier in modifiers {
            match modifier {
                GroupByWithModifier::Rollup
                | GroupByWithModifier::Cube
                | GroupByWithModifier::Totals => {}
                GroupByWithModifier::GroupingSets(expr) => {
                    self.push_accumulator();
                    self.resolve_expr(expr)?;
                    let deps = self.pop_accumulator();
                    self.active_scope().group_by_columns.extend(deps);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;


}