use scylla::client::session::Session;

use crate::keyspace::KeyspaceOpts;

pub(crate) mod keyspace;
pub(crate) mod migrated;

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("{0}")]
    Deserialize(String),
    #[error("{0}")]
    Execution(String),
}

pub(crate) async fn exec(session: &Session, query: String) -> Result<(), QueryError> {
    session
        .query_unpaged(query, ())
        .await
        .map_err(|err| QueryError::Execution(err.to_string()))?;
    Ok(())
}
