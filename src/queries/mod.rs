use scylla::transport::errors::QueryError as ScyllaQueryError;
use scylla::Session;

use crate::keyspace::KeyspaceOpts;

pub(crate) mod keyspace;
pub(crate) mod migrated;

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("cql query error: {source}")]
    TransportError {
        #[from]
        source: ScyllaQueryError,
    },
    #[error("{source}")]
    Other {
        #[from]
        source: anyhow::Error,
    },
}

pub(crate) async fn exec(session: &Session, query: String) -> Result<(), QueryError> {
    session.query(query, ()).await?;
    Ok(())
}
