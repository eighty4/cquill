use std::{num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Error, Result};
use scylla::{
    client::{PoolSize, session::Session, session_builder::SessionBuilder},
    errors::NewSessionError,
};
use thiserror::Error;

use crate::ConnectionOpts;

#[derive(Default, Debug)]
pub struct SessionInputs {
    pub hostname: Option<String>,
    pub port: Option<u32>,
    pub connection_timeout: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ssl_certfile: Option<PathBuf>,
    pub ssl_usercert: Option<PathBuf>,
    pub ssl_userkey: Option<PathBuf>,
    pub use_ssl: bool,
    pub validate_ssl: Option<bool>,
}

#[derive(Error, Debug)]
pub enum CreateSessionError {
    #[error("could not configure a database session: {0}")]
    SessionInputsError(String),
    #[error("could not create a database session: {0}")]
    NewSessionError(String),
}

impl From<NewSessionError> for CreateSessionError {
    fn from(err: NewSessionError) -> Self {
        Self::NewSessionError(err.to_string())
    }
}

pub async fn create(inputs: SessionInputs) -> Result<Arc<Session>, CreateSessionError> {
    SessionBuilder::try_from(inputs)
        .map_err(|err| CreateSessionError::SessionInputsError(format!("{err:#}")))?
        .build()
        .await
        .map_err(CreateSessionError::from)
        .map(Arc::new)
}

impl SessionInputs {
    fn validate_ssl(&self) -> bool {
        self.validate_ssl.unwrap_or(true)
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
            ssl_certfile: other.ssl_certfile.clone(),
            ssl_usercert: other.ssl_usercert.clone(),
            ssl_userkey: other.ssl_userkey.clone(),
            use_ssl: other.use_ssl,
            validate_ssl: other.validate_ssl,
        }
    }
}

impl TryFrom<SessionInputs> for SessionBuilder {
    type Error = Error;

    fn try_from(inputs: SessionInputs) -> Result<Self> {
        let address = format!(
            "{}:{}",
            inputs.hostname.as_deref().unwrap_or("127.0.0.1"),
            inputs.port.unwrap_or(9042)
        );
        let builder = SessionBuilder::new()
            .known_node(&address)
            .tls_context(match inputs.use_ssl {
                true => Some(tls::build_config(&inputs)?),
                false => None,
            })
            .pool_size(PoolSize::PerHost(NonZeroUsize::new(1).unwrap()))
            .connection_timeout(Duration::from_secs(
                inputs.connection_timeout.unwrap_or(5).into(),
            ));

        Ok(
            if let Some(username) = inputs.username
                && let Some(password) = inputs.password
            {
                builder.user(username, password)
            } else {
                builder
            },
        )
    }
}

mod tls {
    use anyhow::{Context, Result, anyhow};
    use rustls::client::danger::ServerCertVerifier;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};
    use rustls_pki_types::PrivateKeyDer;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, OnceLock};

    use super::SessionInputs;

    pub fn build_config(inputs: &SessionInputs) -> Result<Arc<ClientConfig>> {
        static SCHEMES: OnceLock<()> = OnceLock::new();
        SCHEMES.get_or_init(|| {
            rustls::crypto::ring::default_provider()
                .install_default()
                .expect("init tls system");
        });
        let builder = if inputs.validate_ssl() {
            ClientConfig::builder().with_root_certificates(build_root_store(&inputs.ssl_certfile)?)
        } else {
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(create_no_verifier())
        };
        let config =
            if let (Some(usercert), Some(userkey)) = (&inputs.ssl_usercert, &inputs.ssl_userkey) {
                builder
                    .with_client_auth_cert(
                        read_certs(usercert).context("reading mTLS user certs")?,
                        read_key(userkey).context("reading mTLS user key")?,
                    )
                    .context("configuring mTLS user cert and key")?
            } else {
                builder.with_no_client_auth()
            };
        Ok(Arc::new(config))
    }

    fn build_root_store(certfile: &Option<PathBuf>) -> Result<RootCertStore> {
        let mut root_store = RootCertStore::empty();
        if let Some(certfile_path) = &certfile {
            for cert in read_certs(certfile_path).context("reading CA cert for root cert store")? {
                root_store
                    .add(cert)
                    .context("adding CA cert to root store")?;
            }
        }
        Ok(root_store)
    }

    fn read_certs(p: &Path) -> Result<Vec<CertificateDer<'static>>> {
        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_reader_iter(&mut BufReader::new(read_file(p)?))
                .collect::<Result<Vec<_>, _>>()
                .with_context(|| format!("reading cert from `{}`", p.to_string_lossy()))?;
        Ok(certs)
    }

    fn read_key(p: &Path) -> Result<PrivateKeyDer<'static>> {
        let key = PrivateKeyDer::from_pem_reader(&mut BufReader::new(read_file(p)?))
            .map_err(|_| anyhow!("no private key in `{}`", p.to_string_lossy()))?;
        Ok(key)
    }

    fn read_file(p: &Path) -> Result<File> {
        File::open(p).with_context(|| format!("opening `{}`", p.to_string_lossy()))
    }

    fn create_no_verifier() -> Arc<dyn ServerCertVerifier> {
        use rustls::client::danger::{
            HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
        };

        #[derive(Debug)]
        struct NoVerifier;

        impl ServerCertVerifier for NoVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &CertificateDer<'_>,
                _intermediates: &[CertificateDer<'_>],
                _server_name: &ServerName<'_>,
                _ocsp_response: &[u8],
                _now: UnixTime,
            ) -> std::result::Result<ServerCertVerified, Error> {
                Ok(ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &CertificateDer<'_>,
                _dss: &DigitallySignedStruct,
            ) -> std::result::Result<HandshakeSignatureValid, Error> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &CertificateDer<'_>,
                _dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, Error> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
                rustls::crypto::ring::default_provider()
                    .signature_verification_algorithms
                    .supported_schemes()
            }
        }
        Arc::new(NoVerifier {})
    }
}

#[cfg(test)]
mod tests {
    use cquill_dev_gen_certs::{gen_mtls_certs, gen_tls_certs};

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
            ssl_certfile: Some("/certfile".into()),
            ssl_usercert: Some("/usercert".into()),
            ssl_userkey: Some("/userkey".into()),
            use_ssl: true,
            validate_ssl: Some(true),
        };
        let expected = SessionInputs {
            hostname: connection_opts.hostname.clone(),
            port: connection_opts.port,
            connection_timeout: connection_opts.connection_timeout,
            password: connection_opts.password.clone(),
            username: connection_opts.username.clone(),
            ssl_certfile: Some("/certfile".into()),
            ssl_usercert: Some("/usercert".into()),
            ssl_userkey: Some("/userkey".into()),
            use_ssl: true,
            validate_ssl: Some(true),
        };
        assert_inputs_eq(SessionInputs::from(&connection_opts), &expected);
        assert_inputs_eq(SessionInputs::from(&Some(connection_opts)), &expected);
    }

    #[tokio::test]
    async fn test_builder_from_inputs_for_standard_tls() {
        let certs = gen_tls_certs();
        use scylla::cluster::KnownNode;
        let builder = SessionBuilder::try_from(SessionInputs {
            hostname: Some("144.21.21.8".into()),
            port: Some(4044),
            connection_timeout: Some(5),
            password: Some("tahini".into()),
            username: Some("hoyle".into()),
            ssl_certfile: Some(certs.ca_cert.clone()),
            ssl_usercert: None,
            ssl_userkey: None,
            use_ssl: true,
            validate_ssl: Some(true),
        })
        .unwrap();
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

    #[tokio::test]
    async fn test_builder_from_inputs_for_mutual_tls() {
        let certs = gen_mtls_certs();
        use scylla::cluster::KnownNode;
        let builder = SessionBuilder::try_from(SessionInputs {
            hostname: Some("144.21.21.8".into()),
            port: Some(4044),
            connection_timeout: Some(5),
            password: Some("tahini".into()),
            username: Some("hoyle".into()),
            ssl_certfile: Some(certs.ca_cert.clone()),
            ssl_usercert: Some(certs.client_cert.clone()),
            ssl_userkey: Some(certs.client_key.clone()),
            use_ssl: true,
            validate_ssl: Some(true),
        })
        .unwrap();
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
