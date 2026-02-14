use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::ResolvedColumn;
use crate::resolve::{Resolver, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{Expr, Ident, Select, SelectItem, SelectItemQualifiedWildcardKind};
use std::collections::HashSet;

impl<'a, T: SchemaProvider> Resolver<T> {
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
            self.resolve_expr(selection, &mut None)?;
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

                    let mut accumulator: HashSet<String> = HashSet::new();
                    self.resolve_expr(expr, &mut Some(&mut accumulator))?;

                    self.active_scope().outer_columns.push(ResolvedColumn{
                        name: outer_name.clone(),
                        dependencies: accumulator,
                    });

                    resolved_items.push(SelectItem::UnnamedExpr(expr.clone()));
                }
                SelectItem::ExprWithAlias { expr, alias} => {
                    let outer_name = alias.value.clone();

                    let mut accumulator: HashSet<String> = HashSet::new();
                    self.resolve_expr(expr, &mut Some(&mut accumulator))?;

                    self.active_scope().outer_columns.push(ResolvedColumn{
                        name: outer_name.clone(),
                        dependencies: accumulator,
                    });

                    resolved_items.push(SelectItem::ExprWithAlias { expr: expr.clone(), alias: alias.clone() });
                }
                SelectItem::QualifiedWildcard(item, options) => {
                    match item {
                        SelectItemQualifiedWildcardKind::ObjectName(obj_name) => {

                            let expanded_columns = self.resolve_wildcard(Some(obj_name), Some(options))?;

                            for column in expanded_columns {
                                let mut accumulator: HashSet<String> = HashSet::with_capacity(1);
                                accumulator.insert(format!("{}.{}", column.0, column.1));

                                self.active_scope().outer_columns.push(ResolvedColumn{
                                    name: column.1.clone(),
                                    dependencies: accumulator
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
                        let mut accumulator: HashSet<String> = HashSet::with_capacity(1);
                        accumulator.insert(format!("{}.{}", column.0, column.1));

                        self.active_scope().outer_columns.push(ResolvedColumn{
                            name: column.1.clone(),
                            dependencies: accumulator
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

        // HAVING
        if let Some(having) = &mut select.having {
            self.resolve_expr(having, &mut None)?;
        }
        // LATERAL VIEWs
        for lateral in &mut select.lateral_views {
            self.resolve_expr(&mut lateral.lateral_view, &mut None)?;
        }

        if create_scope {
            self.exit_scope();
        }

        // TODO: Choosing to skip the rest for now. Maybe revisit later. :0
        Ok(())
    }
}