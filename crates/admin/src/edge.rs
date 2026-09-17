//! The `aws-sdk-cloudfrontkeyvaluestore`-backed [`EdgeConfigStore`]
//! implementation (issue #71): reads and writes the `CloudFront` Function
//! gate's config document (`c`) in the `KeyValueStore` the function reads
//! from.
//!
//! `PutKey` requires an `IfMatch` `ETag`, and that `ETag` must name the store
//! version the read observed — not a freshly-described one. `GetKey` returns
//! the value but no `ETag`; only `DescribeKeyValueStore` exposes one. So
//! `read_config` pairs `DescribeKeyValueStore` with `GetKey` and returns the
//! read-time `ETag` alongside the value, and `write_config` puts under that
//! read-time `ETag`. The put therefore guards the whole read→put window the
//! read-modify-write spans: a concurrent `PutKey` landing between the read
//! and the put bumps the store's `ETag`, so this caller's now-stale `IfMatch`
//! fails with a `ConflictException` and the caller re-reads and re-applies,
//! rather than the put matching a freshly-described post-concurrent `ETag`
//! and silently clobbering the concurrent change with a stale document.
//!
//! Because `c` also carries `enforce_from` and the ruleset, which nothing in
//! this crate writes yet, every write here is a read-modify-write of the
//! whole document rather than a blind overwrite.

use aws_sdk_cloudfrontkeyvaluestore::Client;

use crate::{ETag, EdgeConfigStore, EdgeStoreError, GateConfig, encode_gate_config};

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

    /// The store's current `ETag`, paired with `GetKey` in `read_config` so the
    /// version the write's `IfMatch` carries names the value the caller's
    /// mutation was derived from rather than whatever a concurrent writer may
    /// have advanced the store to by write time.
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
}

impl EdgeConfigStore for KvsStore {
    async fn read_config(&self) -> Result<(GateConfig, ETag), EdgeStoreError> {
        // Describe immediately before `GetKey` so the `ETag` names a store
        // state in which the returned value was present. A concurrent
        // `PutKey` landing in the (narrow) describe→get_key window bumps the
        // store's `ETag`, so the read-time `ETag` held here names a
        // superseded version: the caller's `write_config` `IfMatch` then
        // fails with a `ConflictException`, which the caller re-reads on
        // rather than clobbering — fails safe in the reject direction.
        let etag = self.etag().await?;
        let out = self
            .client
            .get_key()
            .kvs_arn(&self.kvs_arn)
            .key(CONFIG_KEY)
            .send()
            .await
            .map_err(|e| EdgeStoreError(format!("get_key: {e}")))?;
        let cfg = serde_json::from_str(out.value())
            .map_err(|e| EdgeStoreError(format!("decode gate config: {e}")))?;
        Ok((cfg, ETag(etag)))
    }

    async fn write_config(&self, etag: &ETag, cfg: &GateConfig) -> Result<(), EdgeStoreError> {
        let encoded = encode_gate_config(cfg).map_err(|e| EdgeStoreError(e.to_string()))?;
        // Put under the *read-time* `ETag`, not a freshly-described one: the
        // `IfMatch` succeeds only when no concurrent `PutKey` has bumped the
        // store since the read, so a stale document can never overwrite a
        // concurrent writer's committed change. A `ConflictException`
        // (precondition mismatch) surfaces as `Err` and is handled by the
        // caller's re-read; it is deliberately not retried here, because
        // retrying against a fresh `ETag` would reintroduce exactly the
        // read→put clobber this write exists to prevent.
        self.client
            .put_key()
            .kvs_arn(&self.kvs_arn)
            .key(CONFIG_KEY)
            .value(encoded)
            .if_match(&etag.0)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| EdgeStoreError(format!("put_key: {e}")))
    }
}
