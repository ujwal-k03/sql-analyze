use crate::resolve::errors::ResolutionError;
use crate::resolve::{ResolvedScope, ScopeId};
use crate::schema::TableSchema;
use sqlparser::ast::{ObjectNamePart, TableAlias};

pub trait Source<'a> {
    fn get_correlation_name(&self) -> String;
    fn match_name(&self, ident: &[ObjectNamePart]) -> Result<bool, ResolutionError>;
    fn match_col(&self, col_name: &str) -> Result<bool, ResolutionError> ;
    fn list_cols(&self) -> &[String];
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ResolvedSource {
    Cte(ScopeSource),
    DerivedTable(ScopeSource),
    Table(TableSource),
}

impl<'a> Source<'a> for ResolvedSource {
    fn get_correlation_name(&self) -> String {
        match self {
            ResolvedSource::Cte(scope_source) => scope_source.get_correlation_name(),
            ResolvedSource::DerivedTable(scope_source) => scope_source.get_correlation_name(),
            ResolvedSource::Table(scope_source) => scope_source.get_correlation_name(),
        }
    }

    fn match_name(&self, name: &[ObjectNamePart]) -> Result<bool, ResolutionError> {
        match self {
            ResolvedSource::Cte(s) => s.match_name(name),
            ResolvedSource::DerivedTable(s) => s.match_name(name),
            ResolvedSource::Table(s) => s.match_name(name),
        }
    }

    fn match_col(&self, col_name: &str) -> Result<bool, ResolutionError> {
        match self {
            ResolvedSource::Cte(s) => s.match_col(col_name),
            ResolvedSource::DerivedTable(s) => s.match_col(col_name),
            ResolvedSource::Table(s) => s.match_col(col_name),
        }
    }

    fn list_cols(&self) -> &[String] {
        match self {
            ResolvedSource::Cte(s) => s.list_cols(),
            ResolvedSource::DerivedTable(s) => s.list_cols(),
            ResolvedSource::Table(s) => s.list_cols(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TableSource {
    pub ident: Vec<String>,
    pub columns: Vec<String>,
}

impl<'a> Source<'a> for TableSource {
    fn get_correlation_name(&self) -> String {
        self.ident.last().unwrap().clone()
    }

    fn match_name(
        &self,
        name: &[ObjectNamePart]
    ) -> Result<bool, ResolutionError> {
        let self_ident_len = self.ident.len();
        for (i, part) in name.iter().rev().enumerate() {
            let ident = match part {
                ObjectNamePart::Identifier(ident) => &ident.value,
                ObjectNamePart::Function(_) => return Err(ResolutionError::UnsupportedObjectNamePart(part.clone())),
            };
            if (i > self_ident_len - 1) || self.ident[self_ident_len - 1 - i] != *ident {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn match_col(&self, col_name: &str) -> Result<bool, ResolutionError> {
        let mut cnt = 0;
        for col in self.columns.iter() {
            if col == col_name {
                cnt += 1;
            }
        }

        match cnt {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ResolutionError::AmbiguousColumn(format!("{scope_name}.{col_name}", scope_name=self.ident.join("."), col_name=col_name))),
        }
    }

    fn list_cols(&self) -> &[String] {
        self.columns.as_slice()
    }
}

impl TableSource {
    pub fn from_schema(
        ident: Vec<String>,
        schema: TableSchema,
        alias: &Option<TableAlias>,
    ) -> Result<TableSource, ResolutionError> {
        let mut columns: Vec<String> = schema.columns;

        if let Some(table_alias) = alias {
            if table_alias.columns.len() > 0 {
                if table_alias.columns.len() != columns.len() {
                    return Err(ResolutionError::AliasLengthMismatch(table_alias.to_string()));
                } else {
                    for (idx, column) in table_alias.columns.iter().enumerate() {
                        columns[idx] = column.name.value.clone();
                    }
                }
            }
        }

        Ok(TableSource{
            ident,
            columns,
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopeSource {
    pub name: String,
    pub columns: Vec<String>,
    pub scope_id: ScopeId,
}

impl<'a> Source<'a> for ScopeSource {
    fn get_correlation_name(&self) -> String {
        self.name.clone()
    }

    fn match_name(
        &self,
        name: &[ObjectNamePart]
    ) -> Result<bool, ResolutionError> {
        let self_ident_len = 1;
        for (i, part) in name.iter().rev().enumerate() {
            let ident = match part {
                ObjectNamePart::Identifier(ident) => &ident.value,
                ObjectNamePart::Function(_) => {return Err(ResolutionError::UnsupportedObjectNamePart(part.clone()))}
            };
            if (i > self_ident_len - 1) || self.name != *ident {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn match_col(&self, col_name: &str) -> Result<bool, ResolutionError> {
        let mut cnt = 0;
        for col in self.columns.iter() {
            if col == col_name {
                cnt += 1;
            }
        }

        match cnt {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ResolutionError::AmbiguousColumn(format!("{scope_name}.{col_name}", scope_name=self.name, col_name=col_name))),
        }
    }

    fn list_cols(&self) -> &[String] {
        self.columns.as_slice()
    }
}

impl ScopeSource {
    pub fn from_scope(
        scope: &ResolvedScope,
        name: &str,
        alias: &Option<TableAlias>
    ) -> Result<ScopeSource, ResolutionError> {
        let mut columns: Vec<String> = Vec::with_capacity(scope.outer_columns.len());
        let correlation_name = if let Some(table_alias) = alias {
            table_alias.name.value.clone()
        } else {
            name.to_string()
        };

        if let Some(table_alias) = alias && table_alias.columns.len() > 0 {
            if table_alias.columns.len() != scope.outer_columns.len() {
                return Err(ResolutionError::AliasLengthMismatch(table_alias.to_string()));
            } else {
                for column in table_alias.columns.iter() {
                    columns.push(column.name.value.clone());
                }
            }
        } else {
            for resolved_col in scope.outer_columns.iter() {
                columns.push(resolved_col.name.clone());
            }
        }

        Ok(ScopeSource{
            name: correlation_name,
            columns,
            scope_id: scope.id,
        })
    }
}