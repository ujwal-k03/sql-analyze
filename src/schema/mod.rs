pub mod provider;

pub struct TableSchema {
    pub(crate) columns: Vec<String>
}

pub trait SchemaProvider {
    fn get_schema(&self, ident: &Vec<String>) -> Option<TableSchema>;
}