use configparser::ini::Ini;
use scylla::client::session::Session;

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::exists, path::Path};

use anyhow::{Context, Result, anyhow};

use crate::CqlshrcOpts;
use crate::session::{self, CreateSessionError, SessionInputs};

pub async fn session_from_cqlshrc(opts: &CqlshrcOpts) -> Result<Arc<Session>, CreateSessionError> {
    match session_inputs_from_cqlshrc(opts) {
        Err(err) => Err(CreateSessionError::SessionInputsError(err.to_string())),
        Ok(inputs) => session::create(inputs).await,
    }
}

fn session_inputs_from_cqlshrc(opts: &CqlshrcOpts) -> Result<SessionInputs> {
    if let Some(p) = &opts.path {
        match exists(p) {
            Ok(true) => {}
            _ => return Err(anyhow!("cqlshrc not found at `{}`", p.to_string_lossy()))?,
        }
    }
    let mut inputs = SessionInputs::from(&opts.overrides);
    let cqlshrc_path = get_cqlrshc_path(&opts.path)?;
    let cqlshrc_ini = parse_cqlshrc(&cqlshrc_path)?;
    inputs
        .merge_cqlshrc_ini(cqlshrc_ini)
        .with_context(|| format!("parsing a value from `{}`", cqlshrc_path.to_string_lossy()))?;
    Ok(inputs)
}

fn get_cqlrshc_path(p: &Option<PathBuf>) -> Result<PathBuf> {
    p.clone().map_or_else(default_cqlshrc, Ok)
}

fn default_cqlshrc() -> Result<PathBuf> {
    env::home_dir()
        .ok_or_else(|| {
            anyhow!(
                "could not resolve {} variable in environment to resolve ~/.cassandra directory",
                if cfg!(windows) { "USERPROFILE" } else { "HOME" }
            )
        })
        .map(|p| p.join(".cassandra").join("cqlshrc"))
}

fn parse_cqlshrc(p: &Path) -> Result<Ini> {
    let mut parsed = Ini::new();
    parsed.load(p).map_err(|err| {
        anyhow!(
            "parsing cqlrshc ini format of `{}` config: {err}",
            p.to_string_lossy()
        )
    })?;
    Ok(parsed)
}

impl SessionInputs {
    // merge cqlshrc ini under existing cli and env values
    fn merge_cqlshrc_ini(&mut self, cqlshrc: Ini) -> Result<()> {
        if self.hostname.is_none() {
            self.hostname = cqlshrc.get("connection", "hostname");
        }
        if self.port.is_none()
            && let Some(port_s) = cqlshrc.get("connection", "port")
        {
            self.port = Some(port_s.parse().map_err(|_| {
                anyhow!(r#"could not parse `connection.port` value "{port_s}" as u32"#)
            })?);
        }
        if self.connection_timeout.is_none()
            && let Some(timeout_s) = cqlshrc.get("connection", "timeout")
        {
            self.connection_timeout = Some(timeout_s.parse().map_err(|_| {
                anyhow!(r#"could not parse `connection.timeout` value "{timeout_s}" as u16"#)
            })?);
        }
        if self.username.is_none() {
            self.username = cqlshrc.get("authentication", "username");
        }
        if self.password.is_none() {
            self.password = cqlshrc.get("authentication", "password");
        }
        if self.ssl_certfile.is_none() {
            self.ssl_certfile = cqlshrc.get("ssl", "certfile").map(PathBuf::from);
        }
        if self.ssl_usercert.is_none() {
            self.ssl_usercert = cqlshrc.get("ssl", "usercert").map(PathBuf::from);
        }
        if self.ssl_userkey.is_none() {
            self.ssl_userkey = cqlshrc.get("ssl", "userkey").map(PathBuf::from);
        }
        if self.validate_ssl.is_none() {
            self.validate_ssl = match cqlshrc.get("ssl", "validate").as_deref() {
                Some("true") => Some(true),
                Some("false") => Some(false),
                Some(bogus) => {
                    return Err(anyhow!(
                        r#"`ssl.validate` value "{bogus}" must be "true" or "false""#
                    ));
                }
                None => None,
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inputs_merge_cqlshrc_port_number_parse_errors() {
        let mut inputs = SessionInputs::default();
        let mut cqlshrc = Ini::new();
        cqlshrc.set("connection", "port", Some("five".into()));
        assert!(inputs.merge_cqlshrc_ini(cqlshrc).is_err());
    }

    #[test]
    fn test_inputs_merge_cqlshrc_connection_timeout_number_parse_errors() {
        let mut inputs = SessionInputs::default();
        let mut cqlshrc = Ini::new();
        cqlshrc.set("connection", "timeout", Some("five".into()));
        assert!(inputs.merge_cqlshrc_ini(cqlshrc).is_err());
    }

    #[test]
    fn test_inputs_merge_cqlshrc_precedence_inputs_over_cqlshrc() {
        let mut inputs = SessionInputs {
            hostname: Some("144.21.21.8".into()),
            port: Some(80085),
            connection_timeout: Some(5),
            password: Some("tahini".into()),
            username: Some("hoyle".into()),
            ssl_certfile: Some(PathBuf::from("/certfile")),
            ssl_usercert: Some(PathBuf::from("/usercert")),
            ssl_userkey: Some(PathBuf::from("/userkey")),
            use_ssl: true,
            validate_ssl: Some(true),
        };
        let mut cqlshrc = Ini::new();
        cqlshrc.set("connection", "hostname", Some("8.21.21.144".into()));
        cqlshrc.set("connection", "port", Some("58008".into()));
        cqlshrc.set("connection", "timeout", Some("10".into()));
        cqlshrc.set("authentication", "username", Some("taco".into()));
        cqlshrc.set("authentication", "password", Some("night".into()));
        cqlshrc.set("ssl", "validate", Some("false".into()));
        cqlshrc.set("ssl", "certfile", Some("/not/certfile".into()));
        cqlshrc.set("ssl", "usercert", Some("/not/usercert".into()));
        cqlshrc.set("ssl", "userkey", Some("/not/userkey".into()));
        assert!(inputs.merge_cqlshrc_ini(cqlshrc).is_ok());
        assert_eq!(inputs.hostname, Some("144.21.21.8".into()));
        assert_eq!(inputs.port, Some(80085));
        assert_eq!(inputs.connection_timeout, Some(5));
        assert_eq!(inputs.password, Some("tahini".into()));
        assert_eq!(inputs.username, Some("hoyle".into()));
        assert_eq!(inputs.ssl_certfile, Some(PathBuf::from("/certfile")));
        assert_eq!(inputs.ssl_usercert, Some(PathBuf::from("/usercert")));
        assert_eq!(inputs.ssl_userkey, Some(PathBuf::from("/userkey")));
        assert!(inputs.use_ssl);
        assert_eq!(inputs.validate_ssl, Some(true));
    }

    #[test]
    fn test_inputs_merge_cqlshrc_precedence_cqlshrc_into_none() {
        let mut inputs = SessionInputs::default();
        let mut cqlshrc = Ini::new();
        cqlshrc.set("connection", "hostname", Some("team-ring".into()));
        cqlshrc.set("connection", "port", Some("5771".into()));
        cqlshrc.set("connection", "timeout", Some("3".into()));
        cqlshrc.set("authentication", "username", Some("champion".into()));
        cqlshrc.set("authentication", "password", Some("hallOfFamer".into()));
        cqlshrc.set("ssl", "validate", Some("true".into()));
        cqlshrc.set("ssl", "certfile", Some("/certfile".into()));
        cqlshrc.set("ssl", "usercert", Some("/usercert".into()));
        cqlshrc.set("ssl", "userkey", Some("/userkey".into()));
        assert!(inputs.merge_cqlshrc_ini(cqlshrc).is_ok());
        assert_eq!(inputs.hostname, Some("team-ring".into()));
        assert_eq!(inputs.port, Some(5771));
        assert_eq!(inputs.connection_timeout, Some(3));
        assert_eq!(inputs.password, Some("hallOfFamer".into()));
        assert_eq!(inputs.username, Some("champion".into()));
        assert_eq!(inputs.ssl_certfile, Some(PathBuf::from("/certfile")));
        assert_eq!(inputs.ssl_usercert, Some(PathBuf::from("/usercert")));
        assert_eq!(inputs.ssl_userkey, Some(PathBuf::from("/userkey")));
        assert!(!inputs.use_ssl);
        assert_eq!(inputs.validate_ssl, Some(true));
    }
}
