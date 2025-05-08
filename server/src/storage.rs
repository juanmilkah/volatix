use std::collections::HashMap;

#[derive(Default)]
pub struct Storage {
    pub store: HashMap<String, String>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: HashMap::new(),
        }
    }

    pub fn get_key(&self, key: &str) -> Option<String> {
        self.store.get(key).cloned()
    }

    pub fn insert_key(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    pub fn remove_key(&mut self, key: &str) {
        self.store.remove(key);
    }
}
