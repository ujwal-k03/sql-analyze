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

use std::collections::HashSet;

use crate::resolve::errors::ResolutionError;
use crate::resolve::scope::{ColumnRef, ResolvedScope, ScopeType};
use crate::schema::SchemaProvider;

type ScopeId = usize;

pub struct ResolutionOptions {
    pub expand_select_wildcards: bool,
    pub qualify: bool,
}

pub struct Resolver<T: SchemaProvider> {
    pub(crate) schema_provider: T,
    pub options: ResolutionOptions,
}

impl<T: SchemaProvider> Resolver<T> {
    pub fn new(schema_provider: T, options: ResolutionOptions) -> Resolver<T> {
        Resolver { schema_provider, options }
    }

    pub fn resolve(
        &self,
        statement: &mut Statement,
    ) -> Result<Vec<ResolvedScope>, ResolutionError> {
        let mut cx = ResolutionContext::new(self);
        cx.resolve_statement(statement)?;
        Ok(cx.scopes)
    }
}

pub(crate) struct ResolutionContext<'r, T: SchemaProvider> {
    pub(crate) resolver: &'r Resolver<T>,
    pub scopes: Vec<ResolvedScope>,
    pub active_scope: ScopeId,
    pub visible_scopes: Vec<ScopeId>,
    accumulator_stack: Vec<HashSet<ColumnRef>>,
}

impl<'r, T: SchemaProvider> ResolutionContext<'r, T> {
    /// Initialize a context with a single boundary scope.
    ///
    /// This initial boundary scope helps avoid Option<ScopeId> in the code
    fn new(resolver: &'r Resolver<T>) -> ResolutionContext<'r, T> {
        ResolutionContext {
            resolver,
            scopes: vec![ResolvedScope::new(0, 0, ScopeType::Boundary, false)],
            active_scope: 0,
            visible_scopes: vec![0],
            accumulator_stack: Vec::new(),
        }
    }

    /// Pushes a fresh accumulator onto the column-dependency stack.
    /// Pair with `pop_accumulator` to retrieve the collected set.
    pub(crate) fn push_accumulator(&mut self) {
        self.accumulator_stack.push(HashSet::new());
    }

    /// Pops the top accumulator. Panics if the stack is empty.
    pub(crate) fn pop_accumulator(&mut self) -> HashSet<ColumnRef> {
        self.accumulator_stack.pop().expect("pop_accumulator with empty stack")
    }

    /// Records a column reference into every active accumulator frame.
    /// Base-column dependencies propagate through subquery boundaries — the
    /// outer expression's lineage is the flat union of every base column
    /// touched while resolving it, regardless of nesting depth. No-op when
    /// no frame is active.
    pub(crate) fn record_column(&mut self, col: ColumnRef) {
        for frame in self.accumulator_stack.iter_mut() {
            frame.insert(col.clone());
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

        self.scopes.push(ResolvedScope::new(new_id, parent_id, scope_type, allow_lateral));

        self.scopes[parent_id].children.push(new_id);
        self.active_scope = new_id;
        self.visible_scopes.push(new_id);
    }

    /// Exits the current scope into its parent.
    ///
    /// # Returns
    /// ScopeId - the scope id of the exited scope
    pub(crate) fn exit_scope(
        &mut self,
    ) -> ScopeId {
        let old_scope = self.active_scope;
        self.active_scope = self.active_scope().parent;
        self.visible_scopes.pop();
        old_scope
    }

    fn resolve_statement(
        &mut self,
        statement: &mut Statement,
    ) -> Result<(), ResolutionError> {
        match statement {
            Statement::Query(query) => {
                // Every query creates a new scope
                // TODO [Optimization]: In case of parenthesised queries like ((SELECT 1))
                // there is no point of creating a new scope for each set of parentheses
                self.resolve_query(query, ScopeType::Root, false)?;
            }
            _ => {
                return Err(ResolutionError::UnsupportedQueryType(std::any::type_name::<Statement>().into()));
            }
        }
        Ok(())
    }
}