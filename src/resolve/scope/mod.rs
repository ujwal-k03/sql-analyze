use crate::resolve::scope::sources::ResolvedSource;
use crate::resolve::ScopeId;
use std::collections::{HashMap, HashSet};

pub mod sources;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SelectedColumn {
    pub name: String,
    pub dependencies: HashSet<ColumnRef>
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub name: String,
    pub source_name: String
}

#[derive(Copy, Clone, serde::Serialize, serde::Deserialize)]
pub enum ScopeType {
    Boundary,
    Root,
    Subquery,
    DerivedTable,
    Cte,
    SetOpBranch,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ResolvedScope {
    pub id: ScopeId,
    pub children: Vec<ScopeId>,
    pub parent: ScopeId,
    pub scope_type: ScopeType,
    pub selected_columns: Vec<SelectedColumn>,
    pub sources: HashMap<String, ResolvedSource>,
    pub join_columns: HashSet<ColumnRef>,
    pub group_by_columns: HashSet<ColumnRef>,
    pub filter_columns: HashSet<ColumnRef>,
    pub sort_columns: HashSet<ColumnRef>,
    pub allow_lateral: bool,
    pub ctes: HashMap<String, ScopeId>
}