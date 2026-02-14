use crate::resolve::scope::sources::ResolvedSource;
use crate::resolve::ScopeId;
use std::collections::{HashMap, HashSet};

pub mod sources;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ResolvedColumn {
    pub name: String,
    pub dependencies: HashSet<String>
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
    pub outer_columns: Vec<ResolvedColumn>,
    pub sources: HashMap<String, ResolvedSource>,
    pub allow_lateral: bool,
    pub ctes: HashMap<String, ScopeId>
}