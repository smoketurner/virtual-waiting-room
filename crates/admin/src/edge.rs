//! The `aws-sdk-cloudfrontkeyvaluestore`-backed [`EdgeConfigStore`]
//! implementation (issue #71): reads and writes the `CloudFront` Function
//! gate's config document (`c`) in the `KeyValueStore` the function reads
//! from.
//!
//! `PutKey` requires an `IfMatch` `ETag` from `DescribeKeyValueStore`, so a
//! write is describe-then-put — and because `c` also carries `enforce_from`
//! and the ruleset, which nothing in this crate writes yet, every write here
//! is a read-modify-write of the whole document rather than a blind
//! overwrite.

use aws_sdk_cloudfrontkeyvaluestore::Client;

use crate::{EdgeConfigStore, EdgeStoreError, GateConfig, encode_gate_config};

const CONFIG_KEY: &str = "c";

/// A live KeyValueStore-backed [`EdgeConfigStore`] bound to one store.
pub struct KvsStore {
    client: Client,
    kvs_arn: String,
}

impl KvsStore {
    #[must_use]
    pub fn new(client: Client, kvs_arn: String) -> Self {
        Self { client, kvs_arn }
    }

    /// The store's current `ETag`, the precondition every `PutKey` requires.
    async fn etag(&self) -> Result<String, EdgeStoreError> {
        let out = self
            .client
            .describe_key_value_store()
            .kvs_arn(&self.kvs_arn)
            .send()
            .await
            .map_err(|e| EdgeStoreError(format!("describe_key_value_store: {e}")))?;
        Ok(out.e_tag().to_owned())
    }

    /// One `PutKey` attempt under a freshly-described `ETag`.
    async fn try_put(&self, value: &str) -> Result<(), EdgeStoreError> {
        let etag = self.etag().await?;
        self.client
            .put_key()
            .kvs_arn(&self.kvs_arn)
            .key(CONFIG_KEY)
            .value(value)
            .if_match(etag)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| EdgeStoreError(format!("put_key: {e}")))
    }
}

impl EdgeConfigStore for KvsStore {
    async fn read_config(&self) -> Result<GateConfig, EdgeStoreError> {
        let out = self
            .client
            .get_key()
            .kvs_arn(&self.kvs_arn)
            .key(CONFIG_KEY)
            .send()
            .await
            .map_err(|e| EdgeStoreError(format!("get_key: {e}")))?;
        serde_json::from_str(out.value())
            .map_err(|e| EdgeStoreError(format!("decode gate config: {e}")))
    }

    async fn write_config(&self, cfg: &GateConfig) -> Result<(), EdgeStoreError> {
        let encoded = encode_gate_config(cfg).map_err(|e| EdgeStoreError(e.to_string()))?;
        // One retry on a conflicting ETag: another writer landed between our
        // describe and our put. The write is a read-modify-write of the whole
        // document, so the loser must re-describe and re-try rather than
        // clobber the winner's change with a stale precondition.
        match self.try_put(&encoded).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "put_key failed; retrying once against a fresh ETag");
                self.try_put(&encoded).await
            }
        }
    }
}
