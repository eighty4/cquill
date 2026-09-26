use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use scylla::client::{session::Session, session_builder::SessionBuilder};

use crate::ConnectionOpts;

#[derive(Default)]
pub struct SessionInputs {
    pub hostname: Option<String>,
    pub port: Option<u32>,
    pub connection_timeout: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
}

pub async fn create(inputs: SessionInputs) -> Result<Arc<Session>> {
    match SessionBuilder::from(inputs).build().await {
        Ok(session) => Ok(Arc::new(session)),
        Err(err) => Err(anyhow!("could not connect to db: {err}")),
    }
}

impl From<&Option<ConnectionOpts>> for SessionInputs {
    fn from(other: &Option<ConnectionOpts>) -> Self {
        match other {
            None => Self::default(),
            Some(opts) => Self::from(opts),
        }
    }
}

impl From<&ConnectionOpts> for SessionInputs {
    fn from(other: &ConnectionOpts) -> Self {
        Self {
            hostname: other.hostname.clone(),
            port: other.port,
            connection_timeout: other.connection_timeout,
            username: other.username.clone(),
            password: other.password.clone(),
        }
    }
}

impl From<SessionInputs> for SessionBuilder {
    fn from(inputs: SessionInputs) -> Self {
        let address = format!(
            "{}:{}",
            inputs.hostname.as_deref().unwrap_or("127.0.0.1"),
            inputs.port.unwrap_or(9042)
        );
        let mut builder = SessionBuilder::new()
            .pool_size(scylla::client::PoolSize::PerHost(
                std::num::NonZeroUsize::new(1).unwrap(),
            ))
            .known_node(&address);
        if let Some(username) = inputs.username
            && let Some(password) = inputs.password
        {
            builder = builder.user(username, password);
        }
        if let Some(connection_timeout) = inputs.connection_timeout {
            builder = builder.connection_timeout(Duration::from_secs(connection_timeout.into()));
        }
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_inputs_eq(actual: SessionInputs, expected: &SessionInputs) {
        assert_eq!(actual.hostname, expected.hostname);
        assert_eq!(actual.port, expected.port);
        assert_eq!(actual.connection_timeout, expected.connection_timeout);
        assert_eq!(actual.password, expected.password);
        assert_eq!(actual.username, expected.username);
    }

    #[test]
    fn test_inputs_from_opts() {
        let connection_opts = ConnectionOpts {
            hostname: Some("144.21.21.8".into()),
            port: Some(4044),
            connection_timeout: Some(5),
            password: Some("tahini".into()),
            username: Some("hoyle".into()),
        };
        let expected = SessionInputs {
            hostname: connection_opts.hostname.clone(),
            port: connection_opts.port,
            connection_timeout: connection_opts.connection_timeout,
            password: connection_opts.password.clone(),
            username: connection_opts.username.clone(),
        };
        assert_inputs_eq(SessionInputs::from(&connection_opts), &expected);
        assert_inputs_eq(SessionInputs::from(&Some(connection_opts)), &expected);
    }

    #[tokio::test]
    async fn test_builder_from_inputs() {
        use scylla::cluster::KnownNode;
        let builder = SessionBuilder::from(SessionInputs {
            hostname: Some("144.21.21.8".into()),
            port: Some(4044),
            connection_timeout: Some(5),
            password: Some("tahini".into()),
            username: Some("hoyle".into()),
        });
        let expected = KnownNode::Hostname("144.21.21.8:4044".into());
        assert_eq!(builder.config.known_nodes.first().unwrap(), &expected);
        assert_eq!(builder.config.connect_timeout.as_secs(), 5);

        let auth_result = builder
            .config
            .authenticator
            .unwrap()
            .start_authentication_session("")
            .await;
        assert!(auth_result.is_ok());
        let (auth_payload, _) = auth_result.unwrap();
        assert_eq!(
            auth_payload,
            Some(
                [
                    vec![0],
                    Vec::from("hoyle".as_bytes()),
                    vec![0],
                    Vec::from("tahini".as_bytes()),
                ]
                .concat()
            )
        );
    }
}
