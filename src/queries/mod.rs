use scylla::deserialize::DeserializationError;
use scylla::transport::errors::QueryError as ScyllaQueryError;
use scylla::transport::query_result::{IntoRowsResultError, RowsError};
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
    #[error("cql result row metadata deserialize error: {source}")]
    RowsDeserializeError {
        #[from]
        source: RowsError,
    },
    #[error("cql result fetch error: {source}")]
    QueryIntoRowsError {
        #[from]
        source: IntoRowsResultError,
    },
    #[error("cql result deserialize error: {source}")]
    ResultDeserializeError {
        #[from]
        source: DeserializationError,
    },
    #[error("{source}")]
    Other {
        #[from]
        source: anyhow::Error,
    },
}

pub(crate) async fn exec(session: &Session, query: String) -> Result<(), QueryError> {
    session.query_unpaged(query, ()).await?;
    Ok(())
}
