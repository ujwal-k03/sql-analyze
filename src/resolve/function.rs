use crate::resolve::errors::ResolutionError;
use crate::resolve::{ResolutionContext, ScopeType};
use crate::schema::SchemaProvider;
use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArguments, WindowFrameBound, WindowType,
};

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

        if let Some(filter) = &mut function.filter {
            self.resolve_expr(filter)?;
        }

        for order_by_expr in &mut function.within_group {
            self.resolve_expr(&mut order_by_expr.expr)?;
        }

        if let Some(over) = &mut function.over {
            match over {
                WindowType::WindowSpec(spec) => {
                    for expr in &mut spec.partition_by {
                        self.resolve_expr(expr)?;
                    }
                    for order_by_expr in &mut spec.order_by {
                        self.resolve_expr(&mut order_by_expr.expr)?;
                    }
                    if let Some(frame) = &mut spec.window_frame {
                        self.resolve_window_frame_bound(&mut frame.start_bound)?;
                        if let Some(end_bound) = &mut frame.end_bound {
                            self.resolve_window_frame_bound(end_bound)?;
                        }
                    }
                }
                WindowType::NamedWindow(_) => {
                    // Named windows reference a WINDOW clause definition; nothing
                    // to walk locally. Resolving the WINDOW clause itself is a
                    // separate gap (no SELECT.windows support yet).
                }
            }
        }

        Ok(())
    }

    fn resolve_window_frame_bound(
        &mut self,
        bound: &mut WindowFrameBound,
    ) -> Result<(), ResolutionError> {
        match bound {
            WindowFrameBound::CurrentRow => {}
            WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                if let Some(expr) = expr {
                    self.resolve_expr(expr)?;
                }
            }
        }
        Ok(())
    }
}
