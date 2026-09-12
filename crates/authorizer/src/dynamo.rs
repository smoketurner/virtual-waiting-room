//! `DynamoDB`-backed side effects for the authorizer, behind a trait so the
//! decision pipeline runs AWS-free in tests.
//!
//! Two effects: recording an arrival when a token becomes a session
//! (`ADD arrivals#<shard>` on `Counters`), and marking a single-use admission
//! token consumed (`Tokens` table, `token#<request_id>` key, `attribute_not_exists`
//! guard, `DynamoDB` TTL self-expiry, matching the session/PKCE convention in
//! the admin crate).

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::Shard;
use wr_common::expr::{Key, SHARD_COUNT_ATTR, SHARD_INDEX_ATTR, TOKENS_TTL_ATTR, Update};

/// A side-effect failure. The handler treats a failure to record an arrival as
/// non-fatal (the visitor is still admitted; the controller tolerates a missed
/// count), and a failure to reserve a token as a denial.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("dynamodb: {0}")]
    Backend(String),
}

/// The effects the authorizer performs, abstracted for testing.
pub trait Store {
    /// Records one arrival on the given shard of the `Counters` item.
    fn record_arrival(
        &self,
        event_id: &str,
        shard: Shard,
    ) -> impl std::future::Future<Output = Result<(), StoreError>> + Send;

    /// Reserves a single-use admission token. Returns `true` if this call
    /// claimed it (first use) and `false` if it was already consumed.
    fn reserve_token(
        &self,
        request_id: &str,
        expires_at: u64,
    ) -> impl std::future::Future<Output = Result<bool, StoreError>> + Send;
}

/// A live store bound to the `Counters` and `Tokens` tables.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
    tokens_table: String,
}

impl DynamoStore {
    #[must_use]
    pub fn new(client: Client, counters_table: String, tokens_table: String) -> Self {
        Self {
            client,
            counters_table,
            tokens_table,
        }
    }
}

impl Store for DynamoStore {
    async fn record_arrival(&self, event_id: &str, shard: Shard) -> Result<(), StoreError> {
        // Stamps the shard's own index alongside the +1 so a reader that
        // fetched a batch of shards knows which is which.
        let bump = Update::new()
            .set(
                SHARD_INDEX_ATTR,
                AttributeValue::N(shard.index().to_string()),
            )
            .add(SHARD_COUNT_ATTR, AttributeValue::N("1".to_owned()))
            .build();
        // The shard is its own item, so arrivals do not contend with the
        // sequences on the Counters item.
        self.client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::ArrivalsShard { event_id, shard }.build()))
            .update_expression(bump.expression)
            .set_expression_attribute_names(Some(bump.names))
            .set_expression_attribute_values(Some(bump.values))
            .send()
            .await
            .map_err(|e| StoreError::Backend(format!("record_arrival: {e}")))?;
        Ok(())
    }

    async fn reserve_token(&self, request_id: &str, expires_at: u64) -> Result<bool, StoreError> {
        let result = self
            .client
            .put_item()
            .table_name(&self.tokens_table)
            .set_item(Some(Key::AdmissionToken { request_id }.build()))
            .item(TOKENS_TTL_ATTR, AttributeValue::N(expires_at.to_string()))
            .condition_expression("attribute_not_exists(request_id)")
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(se))
                if matches!(se.err(), PutItemError::ConditionalCheckFailedException(_)) =>
            {
                Ok(false)
            }
            Err(e) => Err(StoreError::Backend(format!("reserve_token: {e}"))),
        }
    }
}
