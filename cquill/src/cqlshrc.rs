use configparser::ini::Ini;
use scylla::client::session::Session;

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::exists, path::Path};

use anyhow::{Result, anyhow};

use crate::session::SessionInputs;
use crate::{CqlshrcOpts, session};

pub async fn session_from_cqlshrc(opts: &CqlshrcOpts) -> Result<Arc<Session>> {
    if let Some(p) = &opts.path
        && !exists(p)?
    {
        return Err(anyhow!("cqlshrc not found at {}", p.to_string_lossy()));
    }

    let mut inputs = SessionInputs::from(&opts.overrides);
    let cqlshrc_ini = parse_cqlshrc(opts.path.as_deref())?;
    inputs.merge_cqlshrc_ini(cqlshrc_ini)?;
    session::create(inputs).await
}

fn parse_cqlshrc(p: Option<&Path>) -> Result<Ini> {
    let mut parsed = configparser::ini::Ini::new();
    match p {
        None => parsed.load(default_cqlshrc()),
        Some(p) => parsed.load(p),
    }
    .map_err(|err| anyhow!("{err}"))?;
    Ok(parsed)
}

fn default_cqlshrc() -> PathBuf {
    env::home_dir()
        .expect("env var for HOME or USERPROFILE directory")
        .join(".cassandra")
        .join("cqlshrc")
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
            self.port = Some(port_s.parse().map_err(|err| anyhow!("{err}"))?);
        }
        if self.connection_timeout.is_none()
            && let Some(timeout_s) = cqlshrc.get("connection", "timeout")
        {
            self.connection_timeout = Some(timeout_s.parse().map_err(|err| anyhow!("{err}"))?);
        }
        if self.username.is_none() {
            self.username = cqlshrc.get("authentication", "username");
        }
        if self.password.is_none() {
            self.password = cqlshrc.get("authentication", "password");
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
        };
        let mut cqlshrc = Ini::new();
        cqlshrc.set("connection", "hostname", Some("8.21.21.144".into()));
        cqlshrc.set("connection", "port", Some("58008".into()));
        cqlshrc.set("connection", "timeout", Some("10".into()));
        cqlshrc.set("authentication", "username", Some("taco".into()));
        cqlshrc.set("authentication", "password", Some("night".into()));
        assert!(inputs.merge_cqlshrc_ini(cqlshrc).is_ok());
        assert_eq!(inputs.hostname, Some("144.21.21.8".into()));
        assert_eq!(inputs.port, Some(80085));
        assert_eq!(inputs.connection_timeout, Some(5));
        assert_eq!(inputs.password, Some("tahini".into()));
        assert_eq!(inputs.username, Some("hoyle".into()));
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
        assert!(inputs.merge_cqlshrc_ini(cqlshrc).is_ok());
        assert_eq!(inputs.hostname, Some("team-ring".into()));
        assert_eq!(inputs.port, Some(5771));
        assert_eq!(inputs.connection_timeout, Some(3));
        assert_eq!(inputs.password, Some("hallOfFamer".into()));
        assert_eq!(inputs.username, Some("champion".into()));
    }
}
