use std::sync::Arc;
use crate::app::Context;
use serde::Deserialize;
use sqlx::postgres::PgQueryResult;
use tracing::error;
use std::time::Duration;
//use qp_trie::Trie;


const PROPDEFS_BATCH_FETCH_ATTEMPT: &str = "propfilter_batch_fetch_attempt";
const BATCH_RETRY_DELAY_MS: u64 = 100;
const MAX_FETCH_ATTEMPTS: u64 = 5;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord, Deserialize)]
struct TrieEntry {
    property_type: String,
    property_name: String,
}

// property def "key" for insertion or lookup in a Trie.
// impl of serde::Deserialize enables conversion to &[u8]
impl TrieEntry {
    pub fn new(property_type: String, property_name: String) -> Self {
        Self {
            property_type,
            property_name,
        }
    }
}

pub async fn filter_builder(ctx: Arc<Context>, team_id: i64) {
    let mut propdef_next_id: i64 = 0;
    loop {
        let result = get_next_batch(&ctx, team_id, propdef_next_id).await
            .map_err(|e| error!("failed to fetch next batch for team_id {} at propdefs_next_id {} with: {:?}", team_id, propdef_next_id, e));


    }
    // TODO: implement team, propdefs table scanners and filter building
}

async fn get_next_batch(ctx: &Arc<Context>, team_id: i64, propdef_next_id: i64) -> Result<PgQueryResult, sqlx::Error> {
    let mut attempt = 1;
    loop {
        match sqlx::query(r#"
            SELECT property_type, name FROM posthog_propertydefinition
            WHERE team_id = $1 AND id >= $2
            ORDER BY id LIMIT 100"#)
            .bind(team_id)
            .bind(propdef_next_id)
            .execute(&ctx.pool).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempt >= MAX_FETCH_ATTEMPTS {
                        return Err(e);
                    }
                    metrics::counter!(PROPDEFS_BATCH_FETCH_ATTEMPT, &[("result", "retry")])
                        .increment(1);
                    let jitter = rand::random::<u64>() % 50;
                    let delay: u64 = attempt * BATCH_RETRY_DELAY_MS + jitter;
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    attempt += 1;
                },
            }
    }
}