//! CSV-based schema provider: discovers *.csv in a directory and serves column schemas.

use std::collections::HashMap;
use std::path::Path;
use crate::schema::{SchemaProvider, TableSchema};

/// Schema provider that reads table schemas from CSV files in a directory.
/// Each CSV must have a header row with at least a `col_name` column (or first column used as column name).
/// Filename (without `.csv`) is the table identifier, e.g. `platinum.order_master_bi.csv` → ident `["platinum", "order_master_bi"]`.
pub struct CsvSchemaProvider {
    /// Map from table key (ident parts joined by ".") to list of column names.
    tables: HashMap<String, Vec<String>>,
}

impl CsvSchemaProvider {
    /// Initialize by scanning `dir` for `*.csv` files and loading column names from each.
    /// First row is treated as header; column names are taken from the first column (e.g. `col_name`).
    pub fn new(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = dir.as_ref();
        let mut tables = HashMap::new();

        let entries = std::fs::read_dir(dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "csv").unwrap_or(false) {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    let columns = read_columns_from_csv(&path)?;
                    tables.insert(stem.to_string(), columns);
                }
            }
        }

        Ok(Self { tables })
    }
}

fn read_columns_from_csv(path: &Path) -> std::io::Result<Vec<String>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let mut columns = Vec::new();
    for result in rdr.records() {
        let record = result.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(first) = record.get(0) {
            columns.push(first.to_string());
        }
    }
    Ok(columns)
}

impl SchemaProvider for CsvSchemaProvider {
    fn get_schema(&self, ident: &Vec<String>) -> Option<TableSchema> {
        let key = ident.join(".");
        let columns = self
            .tables
            .get(&key);

        if let Some(columns) = columns {
            Some(TableSchema {
                columns: columns.clone(),
            })
        } else {
            None
        }
    }
}
