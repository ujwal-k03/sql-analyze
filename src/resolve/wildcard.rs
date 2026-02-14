use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::sources::Source;
use crate::resolve::Resolver;
use crate::schema::SchemaProvider;
use sqlparser::ast::{ObjectName, WildcardAdditionalOptions};

impl<'a, T: SchemaProvider> Resolver<T> {
    pub(crate) fn resolve_wildcard(
        &mut self,
        qualifier: Option<&ObjectName>,
        options: Option<&WildcardAdditionalOptions>,
    ) -> Result<Vec<(String, String)>, ResolutionError> {
        // Throws an error for now
        match options {
            Some(options) => {
                if options.opt_ilike.is_some() {
                    return Err(ResolutionError::UnsupportedWildcardOption("ILIKE".to_string()));
                }
                if options.opt_exclude.is_some() {
                    return Err(ResolutionError::UnsupportedWildcardOption("EXCLUDE".to_string()));
                }
                if options.opt_except.is_some() {
                    return Err(ResolutionError::UnsupportedWildcardOption("EXCEPT".to_string()));
                }
                if options.opt_replace.is_some() {
                    return Err(ResolutionError::UnsupportedWildcardOption("REPLACE".to_string()));
                }
                if options.opt_rename.is_some() {
                    return Err(ResolutionError::UnsupportedWildcardOption("RENAME".to_string()));
                }
            }
            None => {}
        }

        let mut expanded_columns: Vec<(String, String)> = Vec::new();
        let mut matched_count = 0;

        if self.active_scope().sources.len() == 0 {
            return Err(ResolutionError::NoTablesSelected);
        }

        for (source_alias, source) in &self.active_scope().sources {
            let matched = if let Some(qualifier) = &qualifier {
                source.match_name(&qualifier.0[..])
            } else {
                Ok(true)
            };

            if !matched? {
                continue;
            }

            matched_count += 1;

            for column in source.list_cols() {
                expanded_columns.push((source_alias.clone(), column.clone()));
            }
        }

        if matched_count == 0 {
            Err(ResolutionError::TableNotFound(qualifier.unwrap().to_string()))
        } else {
            Ok(expanded_columns)
        }
    }
}