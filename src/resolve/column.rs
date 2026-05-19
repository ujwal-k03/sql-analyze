use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::sources::Source;
use crate::resolve::scope::ColumnRef;
use crate::resolve::ResolutionContext;
use crate::schema::SchemaProvider;
use sqlparser::ast::{Ident, ObjectNamePart};

impl<'r, T: SchemaProvider> ResolutionContext<'r, T> {
    pub(crate) fn resolve_col(
        &mut self,
        col_ident: &Ident,
        col_source: &[ObjectNamePart],
    ) -> Result<Vec<Ident>, ResolutionError> {
        // Check the current scope for a resolution.
        // If the current scope cannot resolve this column and allow_lateral (LATERAL)
        // is true, then we check its parent too. Break when allow_lateral = false
        for scope_id in self.visible_scopes.iter().rev() {
            let scope = &self.scopes[*scope_id];
            let mut resolved_col: Option<Vec<Ident>> = None;

            // The scope's hashmap maintains the aliased name -> source mapping.
            // The aliased name purely exists to give the source a uniquely addressable
            // key that is useful in some cases (e.g. wildcard expansion). Matching should
            // ALWAYS occur against the original name the user wrote in the query,
            // but the returned ref is qualified with the aliased key so downstream
            // re-writers can round-trip unambiguously.
            for (aliased_name, source) in scope.sources.iter() {
                if source.match_name(col_source)? && source.match_col(col_ident.value.as_str())? {
                    if resolved_col.is_some() {
                        return Err(ResolutionError::AmbiguousColumn(col_ident.value.clone()));
                    } else {
                        resolved_col = Some(vec![Ident::from(aliased_name.as_str()), col_ident.clone()]);
                    }
                }
            }

            // Record this resolved column in accumulator for dependency tracking
            if let Some(resolved_col) = resolved_col {
                self.record_column(ColumnRef {
                    source_name: resolved_col[0].value.clone(),
                    name: resolved_col[1].value.clone(),
                });
                return Ok(resolved_col);
            }

            if !scope.allow_lateral {
                break;
            }
        }
        Err(ResolutionError::ColumnNotFound(col_ident.value.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;


}