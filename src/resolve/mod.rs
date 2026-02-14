pub mod errors;
mod wildcard;
mod select;
mod expr;
mod query;
mod from;
mod function;
mod column;
mod scope;

use sqlparser::ast::Statement;

use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::{ResolvedScope, ScopeType};
use crate::schema::SchemaProvider;

type ScopeId = usize;

pub struct ResolutionOptions {
    pub expand_select_wildcards: bool,
    pub accumulate_dependencies: bool,
}

pub struct Resolver<T: SchemaProvider> {
    schema_provider: T,
    pub scopes: Vec<ResolvedScope>,
    pub active_scope: ScopeId,
    pub visible_scopes: Vec<ScopeId>,
    pub options: ResolutionOptions,
}

impl<'a, T: SchemaProvider> Resolver<T> {
    /// Initialize a resolver with a single boundary scope
    ///
    /// This initial boundary scope helps avoid Option<ScopeId> in the code
    fn new(schema_provider: T, options: ResolutionOptions) -> Resolver<T> {
        Resolver {
            schema_provider,
            scopes: vec![ResolvedScope{
                id: 0,
                children: Default::default(),
                parent: 0,
                scope_type: ScopeType::Boundary,
                outer_columns: Default::default(),
                sources: Default::default(),
                allow_lateral: false,
                ctes: Default::default(),
            }],
            active_scope: 0,
            visible_scopes: vec![0],
            options,
        }
    }

    pub(crate) fn active_scope(&mut self) -> &mut ResolvedScope {
        &mut self.scopes[self.active_scope]
    }

    pub(crate) fn branch_scope(
        &mut self,
        scope_type: ScopeType,
        allow_lateral: bool,
    ) {
        let new_id = self.scopes.len();
        let parent_id = self.active_scope;

        self.scopes.push(ResolvedScope{
            id: new_id,
            children: Vec::new(),
            parent: parent_id,
            scope_type,
            outer_columns: Default::default(),
            sources: Default::default(),
            allow_lateral,
            ctes: Default::default(),
        });


        self.scopes[parent_id].children.push(new_id);
        self.active_scope = new_id;
        self.visible_scopes.push(new_id);
    }

    /// Exits the current scope into its parent.
    ///
    /// # Returns
    /// ScopeId - the scope id of the exited scope
    pub(crate) fn exit_scope (
        &mut self,
    ) -> ScopeId {
        let old_scope = self.active_scope;
        self.active_scope = self.active_scope().parent;
        self.visible_scopes.pop();
        old_scope
    }

    pub fn resolve(
        statement: &mut Statement,
        schema_provider: T,
        options: ResolutionOptions,
    ) -> Result<Vec<ResolvedScope>, ResolutionError> {
        let mut resolver = Resolver::new(schema_provider, options);

        match statement {
            Statement::Query(query) => {
                // Every query creates a new scope
                // TODO [Optimization]: In case of parenthesised queries like ((SELECT 1))
                // there is no point of creating a new scope for each set of parentheses
                resolver.resolve_query(
                    query,
                    ScopeType::Root,
                    false,
                )?;
            }
            _ => {
                return Err(ResolutionError::UnsupportedQueryType(std::any::type_name::<Statement>().into()));
            }
        }

        Ok(resolver.scopes)
    }
}

