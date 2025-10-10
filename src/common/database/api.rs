use crate::database::DatabaseType;

pub struct Database {}

impl Database {
    pub fn new(_database_type: DatabaseType) -> Self {
        Self {}
    }
}
