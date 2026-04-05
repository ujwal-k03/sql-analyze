use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::sources::Source;
use crate::resolve::scope::ColumnRef;
use crate::resolve::Resolver;
use crate::schema::SchemaProvider;
use sqlparser::ast::{Ident, ObjectNamePart};
use std::collections::HashSet;

impl<'a, T: SchemaProvider> Resolver<T> {
    pub(crate) fn resolve_col(
        &mut self,
        ident_vec: &mut Vec<Ident>,
        accumulator: &mut Option<&mut HashSet<ColumnRef>>,
    ) -> Result<Vec<Ident>, ResolutionError> {
        // println!("Resolving col: {:?}", ident_vec);
        let source_name: Vec<ObjectNamePart> = ident_vec.iter().take(ident_vec.len() - 1).map(|x| -> ObjectNamePart {
            ObjectNamePart::Identifier(x.clone())
        }).collect();

        let col_ident= &ident_vec[ident_vec.len() - 1];

        for scope_id in self.visible_scopes.iter().rev() {
            let scope = &self.scopes[*scope_id];

            let mut resolved_col: Option<Vec<Ident>> = None;
            // println!("Trying in scope: {}", scope_id);
            for (aliased_name, source) in scope.sources.iter() {
                // println!("Trying resolution with source: {}", source.get_correlation_name());
                if source.match_name(&source_name[..])? && source.match_col(col_ident.value.as_str())? {
                    if resolved_col.is_some() {
                        return Err(ResolutionError::AmbiguousColumn(col_ident.value.clone()));
                    } else {
                        resolved_col = Some(vec![Ident::from(aliased_name.as_str()), col_ident.clone()]);
                    }
                }
            }

            if let Some(resolved_col) = resolved_col {
                if let Some(accumulator) = accumulator {
                    accumulator.insert(ColumnRef {
                        source_name: resolved_col[0].value.clone(),
                        name: resolved_col[1].value.clone(),
                    });
                }
                return Ok(resolved_col);
            }

            if !scope.allow_lateral {
                break;
            }
        }
        Err(ResolutionError::ColumnNotFound(format!("{:#?}", ident_vec)))
    }
}