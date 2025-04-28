use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};

use crate::app::Context;

use tracing::{error, warn};

use qp_trie::{wrapper::BString, Trie};
use serde::{Serialize, Deserialize};
use sqlx::{postgres::PgRow, FromRow};

// metrics keys
const PROPDEFS_BATCH_FETCH_ATTEMPT: &str = "propfilter_batch_fetch_attempt";

// teams with more than this many property definitions are outliers
// and should be skipped for further property defs processing anyway.
// looking at the distribution of propdefs to teams in the database,
// this feels like reasonable, but we can make final decisions later.
const TEAM_PROPDEFS_CAP: i64 = 100_000;
const _TEAM_PROPDEFS_FILTER_SIZE_CAP: usize = 8192; // 8k as initial limit

// batch size & retry params
const BATCH_FETCH_SIZE: i64 = 1000;
const BATCH_RETRY_DELAY_MS: u64 = 100;
const MAX_BATCH_FETCH_ATTEMPTS: u64 = 5;

#[derive(Clone, Debug, Serialize, Deserialize, FromRow, PartialEq, Eq, Hash)]
struct FilterRow {
    // the team this filter represents
    team_id: i64,
    // the raw bytes of the serialized trie
    trie_bytes: Option<Vec<u8>>,
    // number of property definitions recorded in the trie
    property_count: i32,
    // is this team prohibited from defining any more properties?
    blocked: bool,
    // timestamps for the filter update cron to use to know which teams
    // need the filter to be crawled and updated with new records
    last_updated_at: DateTime<Utc>
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
struct TrieEntry {
    property_type: char,
    group_type_index: char,
    property_name: String,
}

// property def "key" for insertion or lookup in a Trie.
// impl of serde::Deserialize enables conversion to &[u8]
impl TrieEntry {
    pub fn new(property_name: String, property_type: char, group_type_index: char) -> Self {
        Self {
            property_type,
            group_type_index,
            property_name,
        }
    }

    pub fn from_row(row: PropertyRow) -> Self {
        let group_type_index_resolved: char = row
            .group_type_index
            .map_or('X', |gti| char::from_digit(gti as u32, 10).unwrap());

        Self::new(
            row.name,
            char::from_digit(row.r#type as u32, 10).unwrap(),
            group_type_index_resolved,
        )
    }
}

// used to create qp_trie::BString keys for Trie insertion
impl fmt::Display for TrieEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
            "{}{}{}",
            self.property_type,
            self.group_type_index,
            self.property_name
        )
    }
}

#[derive(Deserialize, FromRow, PartialEq, Eq)]
struct PropertyRow {
    team_id: i64,
    name: String,
    r#type: i8,
    group_type_index: Option<i8>,
}

pub async fn filter_builder(ctx: Arc<Context>, mut filter: FilterRow) {
    let mut offset: i64 = 0;
    let mut trie: Trie<BString, ()> = if filter.trie_bytes.is_none() {
            Trie::new()
        } else {
            Trie::from(filter.trie_bytes.unwrap())
        };
    loop {
        if offset >= TEAM_PROPDEFS_CAP {
            warn!(
                "Filter construction for team {} has exceeded {} properties; marking as blocked",
                filter.team_id, TEAM_PROPDEFS_CAP
            );
            // TODO(eli): upsert posthog_propdeffilter row for this team to mark as blocked
        }

        match get_next_batch(&ctx, filter.team_id, offset).await {
            Ok(rows) => {
                for row in &rows {
                    let pd_row = PropertyRow::from_row(row).unwrap();
                    let entry = TrieEntry::from_row(pd_row);
                    trie.insert_str(&entry.to_string(), ());
                }

                // if we've processed all the rows, we're done
                if rows.is_empty() {
                    // TODO(eli): insert the updated trie into the new filters table!
                    return;
                }

                // iterate on the next batch
                offset += BATCH_FETCH_SIZE;
            }

            Err(_) => return,
        }
    }
}

async fn get_next_batch(
    ctx: &Arc<Context>,
    team_id: i64,
    offset: i64,
) -> Result<Vec<PgRow>, sqlx::Error> {
    let mut attempt = 1;
    // note: I measured this (EXPLAIN, example executions etc.) against several outlier teams
    // that have created millions of hash-based unique property keys and if we cap fetches to
    // 1k and stop iterating at first 100k propdefs, using LIMIT/OFFSET here seems acceptable
    loop {
        match sqlx::query(
            r#"
            SELECT property_type, name, type, group_type_index FROM posthog_propertydefinition
            WHERE team_id = $1
            LIMIT $2 OFFSET $3"#,
        )
        .bind(team_id)
        .bind(BATCH_FETCH_SIZE)
        .bind(offset)
        .fetch_all(&ctx.pool)
        .await
        {
            Ok(rows) => {
                metrics::counter!(PROPDEFS_BATCH_FETCH_ATTEMPT, &[("result", "success")])
                    .increment(1);
                return Ok(rows);
            }
            Err(e) => {
                if attempt >= MAX_BATCH_FETCH_ATTEMPTS {
                    metrics::counter!(PROPDEFS_BATCH_FETCH_ATTEMPT, &[("result", "failed")])
                        .increment(1);
                    error!(
                        "failed to fetch next batch for team_id {} at offset {} with: {:?}",
                        team_id, offset, e
                    );
                    return Err(e);
                }

                // within retry budget, try again
                metrics::counter!(PROPDEFS_BATCH_FETCH_ATTEMPT, &[("result", "retry")])
                    .increment(1);
                let jitter = rand::random::<u64>() % 50;
                let delay: u64 = attempt * BATCH_RETRY_DELAY_MS + jitter;
                tokio::time::sleep(Duration::from_millis(delay)).await;
                attempt += 1;
            }
        }
    }
}
