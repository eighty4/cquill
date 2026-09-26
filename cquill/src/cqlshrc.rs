use cqlsh_rs::cli::CliArgs;
use cqlsh_rs::config::MergedConfig;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use std::fs::exists;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};

use crate::CqlshrcOpts;

pub async fn session_from_cqlshrc(opts: &CqlshrcOpts) -> Result<Arc<Session>> {
    if let Some(p) = &opts.path
        && !exists(p)?
    {
        return Err(anyhow!("cqlshrc does not exist at {}", p.to_string_lossy()));
    }
    let cqlshrc_config = cqlsh_rs::config::load_config(&cqlshrc_cli_args(opts))?;
    let session = create_session_from_config(&cqlshrc_config).await.unwrap();
    Ok(Arc::new(session))
}

fn cqlshrc_cli_args(opts: &CqlshrcOpts) -> CliArgs {
    CliArgs {
        cqlshrc: opts.path.clone().map(|p| {
            p.as_os_str()
                .to_os_string()
                .into_string()
                .expect("valid utf8 cqlshrc path")
        }),
        host: opts.overrides.hostname.clone(),
        port: opts.overrides.port,
        username: opts.overrides.username.clone(),
        password: opts.overrides.password.clone(),
        color: false,
        no_color: false,
        browser: None,
        ssl: false,
        no_file_io: false,
        debug: false,
        coverage: false,
        execute: None,
        file: None,
        keyspace: None,
        connect_timeout: None,
        request_timeout: None,
        tty: false,
        completions: None,
        consistency_level: None,
        cqlversion: None,
        disable_history: false,
        encoding: None,
        generate_man: false,
        no_compact: false,
        protocol_version: None,
        secure_connect_bundle: None,
        serial_consistency_level: None,
    }
}

async fn create_session_from_config(cqlshrc_config: &MergedConfig) -> Result<Session> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        if std::fs::metadata(&cqlshrc_config.host)
            .map(|m| m.file_type().is_socket())
            .unwrap_or(false)
        {
            return Err(anyhow!("unix domain socket address not supported"));
        }
    }
    let node_address = format!("{}:{}", cqlshrc_config.host, cqlshrc_config.port);
    let mut builder = SessionBuilder::new()
        .pool_size(scylla::client::PoolSize::PerHost(
            std::num::NonZeroUsize::new(1).unwrap(),
        ))
        .known_node(&node_address)
        .connection_timeout(Duration::from_secs(cqlshrc_config.connect_timeout));

    if let (Some(username), Some(password)) = (&cqlshrc_config.username, &cqlshrc_config.password) {
        builder = builder.user(username, password);
    }

    match builder.build().await {
        Ok(session) => Ok(session),
        Err(err) => Err(anyhow!("{err}")),
    }
}
