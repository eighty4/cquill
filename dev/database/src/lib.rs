mod container;
mod macros;
mod request;

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use anyhow::{Result, anyhow};
use bollard::{
    Docker,
    plugin::{ContainerCreateBody, HealthStatusEnum, HostConfig},
    query_parameters::CreateImageOptions,
};
use cquill_dev_gen_certs::{MutualTlsCerts, StandardTlsCerts, gen_mtls_certs, gen_tls_certs};
use dtor::dtor;
use futures_util::TryStreamExt;
use tokio::{
    runtime::{Handle, Runtime},
    time::sleep,
};

pub use request::*;

use crate::{
    container::{CQL_PORT, make_container_create_body},
    macros::vec_of_strings,
};

struct RunningContainer {
    id: String,
    details: Arc<DatabaseDetails>,
}

pub struct DatabaseDetails {
    certs: Option<DatabaseCerts>,
    port: u32,
}

#[derive(Debug)]
pub enum DatabaseCerts {
    StandardTls(StandardTlsCerts),
    MutualTls(MutualTlsCerts),
}

impl DatabaseDetails {
    pub fn certs(&self) -> &Option<DatabaseCerts> {
        &self.certs
    }

    pub fn port(&self) -> u32 {
        self.port
    }
}

static CONTAINERS: LazyLock<Mutex<HashMap<DatabaseRequest, RunningContainer>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[dtor(unsafe)]
unsafe fn shutdown_containers() {
    match Handle::try_current() {
        Ok(handle) => {
            handle.block_on(async {
                stop_and_remove_all_containers().await;
            });
        }
        Err(_) => {
            let runtime = Runtime::new().unwrap();
            runtime.block_on(async {
                stop_and_remove_all_containers().await;
            });
        }
    }
}

async fn stop_and_remove_all_containers() {
    let container_ids: Vec<String> = {
        let containers = CONTAINERS.lock().unwrap();
        containers.values().map(|c| c.id.clone()).collect()
    };
    let docker = Docker::connect_with_local_defaults().expect("docker connect");
    for container_id in container_ids {
        if stop_container(&docker, container_id.as_str()).await.is_ok() {
            wait_container(&docker, container_id.as_str()).await;
            _ = docker.remove_container(container_id.as_str(), None).await;
        }
    }
}

pub async fn request_container(request: DatabaseRequest) -> Arc<DatabaseDetails> {
    let container: Option<Arc<DatabaseDetails>> = {
        CONTAINERS
            .lock()
            .unwrap()
            .get(&request)
            .map(|instance| instance.details.clone())
    };
    match container {
        Some(details) => details,
        None => {
            let container = start_container(&request).await.unwrap();
            let details = container.details.clone();
            CONTAINERS.lock().unwrap().insert(request, container);
            details
        }
    }
}

async fn start_container(launching: &DatabaseRequest) -> Result<RunningContainer> {
    let certs = match launching.security {
        Some(DatabaseSecurity::Tls { .. }) => Some(DatabaseCerts::StandardTls(gen_tls_certs())),
        Some(DatabaseSecurity::Mtls) => Some(DatabaseCerts::MutualTls(gen_mtls_certs())),
        _ => None,
    };
    let docker = Docker::connect_with_local_defaults()?;
    if launching.engine.is_scylla() && cfg!(target_os = "macos") {
        initialize_aio_max(&docker).await;
    }
    pull_image(
        &docker,
        CreateImageOptions {
            from_image: Some(launching.engine.image_repo()),
            tag: Some(launching.engine.image_tag()),
            ..Default::default()
        },
    )
    .await?;
    let container_id = docker
        .create_container(None, make_container_create_body(launching, &certs))
        .await?
        .id;
    docker.start_container(&container_id, None).await?;
    let port = get_database_port(&docker, &container_id).await?;
    wait_for_healthy(&docker, &container_id).await;
    Ok(RunningContainer {
        id: container_id,
        details: Arc::new(DatabaseDetails { certs, port }),
    })
}

async fn pull_image(docker: &Docker, create_image_options: CreateImageOptions) -> Result<()> {
    let mut creating_image = docker.create_image(Some(create_image_options), None, None);
    let is_ci = false;
    loop {
        let result = creating_image.try_next().await;
        if is_ci {
            match result {
                Ok(None) => return Ok(()),
                Err(err) => return Err(anyhow!("error pulling image: {err}")),
                _ => continue,
            }
        } else {
            match result {
                Ok(None) | Err(_) => return Ok(()),
                _ => continue,
            }
        }
    }
}

async fn wait_for_healthy(docker: &Docker, container_id: &str) {
    sleep(Duration::from_secs(12)).await;
    loop {
        let container_inspect_response =
            docker.inspect_container(container_id, None).await.unwrap();
        if let Some(state) = container_inspect_response.state
            && let Some(health) = state.health
        {
            match health.status {
                Some(HealthStatusEnum::STARTING) => {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Some(HealthStatusEnum::HEALTHY) => {
                    sleep(Duration::from_secs(2)).await;
                    return;
                }
                _ => panic!(),
            }
        }
    }
}

async fn get_database_port(docker: &Docker, container_id: &str) -> Result<u32> {
    let inspect_container_response = docker.inspect_container(container_id, None).await?;
    if let Some(network_settings) = inspect_container_response.network_settings
        && let Some(ports) = network_settings.ports
        && let Some(Some(port_mappings)) = ports.get(CQL_PORT)
        && let Some(port_mapping) = port_mappings.first()
        && let Some(host_port) = &port_mapping.host_port
    {
        return Ok(host_port.parse::<u32>().expect("valid host port"));
    }
    Err(anyhow!(
        "could not get database port of container {container_id}"
    ))
}

async fn stop_container(docker: &Docker, id: &str) -> Result<()> {
    if let Err(err) = docker.stop_container(id, None).await {
        eprintln!("error stopping container `{id}`: {err}");
    }
    Ok(())
}

async fn wait_container(docker: &Docker, id: &str) {
    let mut wait_stream = docker.wait_container(id, None);
    while let Ok(Some(_)) = wait_stream.try_next().await {}
}

async fn initialize_aio_max(docker: &Docker) {
    pull_image(
        docker,
        CreateImageOptions {
            from_image: Some("alpine".into()),
            tag: Some("latest".into()),
            ..Default::default()
        },
    )
    .await
    .expect("pull image to init aio max");
    let container_id = docker
        .create_container(
            None,
            ContainerCreateBody {
                image: Some(String::from("alpine:latest")),
                host_config: Some(HostConfig {
                    privileged: Some(true),
                    ..Default::default()
                }),
                cmd: Some(vec_of_strings!["sysctl", "-w", "fs.aio-max-nr=1048576"]),
                ..Default::default()
            },
        )
        .await
        .expect("create container to init aio max")
        .id;
    docker
        .start_container(&container_id, None)
        .await
        .expect("start container to init aio max");
    wait_container(docker, &container_id).await;
    docker
        .remove_container(&container_id, None)
        .await
        .expect("remove container to init aio max");
}
