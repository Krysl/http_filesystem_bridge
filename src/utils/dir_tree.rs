use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DirTree {
    pub name: String,
    pub children: Vec<DirTree>,
}

impl DirTree {
    pub fn is_folder(&self) -> bool {
        self.name.ends_with('/')
    }
}
