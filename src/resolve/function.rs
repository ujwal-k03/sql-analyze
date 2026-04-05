use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::ColumnRef;
use crate::resolve::{Resolver, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, FunctionArguments};
use std::collections::HashSet;

impl<'a, T: SchemaProvider> Resolver<T> {
    pub(crate) fn resolve_function(
        &mut self,
        function: &mut Function,
        accumulator: &mut Option<&mut HashSet<ColumnRef>>,
    ) -> Result<(), ResolutionError> {
        match &mut function.args {
            FunctionArguments::None => {}
            FunctionArguments::Subquery(subquery) => {
                self.resolve_query(subquery, ScopeType::Subquery,true)?;
            }
            FunctionArguments::List(list) => {
                for item in &mut list.args {
                    match item {
                        FunctionArg::Named{arg, ..}
                        | FunctionArg::ExprNamed{arg, ..}
                        | FunctionArg::Unnamed(arg) => {
                            match arg {
                                FunctionArgExpr::Expr(expr) => self.resolve_expr(expr, accumulator)?,
                                FunctionArgExpr::Wildcard => {
                                    let expanded_columns = self.resolve_wildcard(None, None)?;

                                    if let Some(accumulator) = accumulator {
                                        for column in expanded_columns {
                                            accumulator.insert(ColumnRef {
                                                source_name: column.0,
                                                name: column.1,
                                            });
                                        }
                                    }
                                }
                                FunctionArgExpr::QualifiedWildcard(obj_name) => {
                                    let expanded_columns = self.resolve_wildcard(Some(obj_name), None)?;

                                    if let Some(accumulator) = accumulator {
                                        for column in expanded_columns {
                                            accumulator.insert(ColumnRef {
                                                source_name: column.0,
                                                name: column.1,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
        // TODO: too much work, will think of this later. :3
    }
}