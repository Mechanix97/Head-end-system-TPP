use uuid::Uuid;

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Connection {
    pub id: u128,
    pub ip: String,
    pub job_id: Option<Uuid>,
}

impl Connection {
    pub fn new(id: u128, ip: String) -> Self {
        Connection {
            id,
            ip,
            job_id: None,
        }
    }
}
