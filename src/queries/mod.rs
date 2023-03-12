use anyhow::{anyhow, Result};
use scylla::Session;

use crate::keyspace::KeyspaceOpts;

pub(crate) mod keyspace;
pub(crate) mod migrated;

#[cfg(test)]
pub(crate) mod test_utils;
