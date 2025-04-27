use crate::config::Config;
use health::{HealthHandle, HealthRegistry};
use sqlx::PgPool;

pub struct Context {
    pub config: Config,
    pub pool: PgPool,
    pub liveness: HealthRegistry,
    pub worker_liveness: HealthHandle,
}
