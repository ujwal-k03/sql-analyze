
pub struct LineageNode {
    pub name: String,
    pub source: String,
    pub downstreams: Vec<LineageNode>,
}