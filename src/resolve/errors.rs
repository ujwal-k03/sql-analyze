use sqlparser::ast::ObjectNamePart;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ResolutionError {
    TableNotFound(String),
    ColumnNotFound(String),
    AmbiguousColumn(String),
    AmbiguousSource(String),
    DuplicateCte(String),
    UnsupportedQueryType(String),
    NoCurrentScope,
    UnsupportedWildcardType(String),
    UnsupportedTableWithArguments(String),
    UnsupportedWildcardOption(String),
    UnsupportedObjectNamePart(ObjectNamePart),
    UnsupportedTableFactor(String),
    AliasLengthMismatch(String),
    InvalidWildcard,
    NoTablesSelected,
}