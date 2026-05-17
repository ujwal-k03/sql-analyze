use crate::resolve::errors::ResolutionError;
use crate::resolve::{ResolutionContext, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, FunctionArguments};

impl<'r, T: SchemaProvider> ResolutionContext<'r, T> {
    pub(crate) fn resolve_function(
        &mut self,
        function: &mut Function,
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
                                FunctionArgExpr::Expr(expr) => self.resolve_expr(expr)?,
                                FunctionArgExpr::Wildcard => {
                                    // Auto-registers expanded columns via record_column.
                                    self.resolve_wildcard(None, None)?;
                                }
                                FunctionArgExpr::QualifiedWildcard(obj_name) => {
                                    self.resolve_wildcard(Some(obj_name), None)?;
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
