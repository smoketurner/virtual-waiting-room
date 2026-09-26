//! The SDK-backed [`ReadinessProbe`] (issue #70): read-only calls against the
//! running deployment, each reduced to the plain facts `readiness` judges.
//!
//! Every call here is a Describe or a Get. The admin role's readiness grants
//! (`infra/modules/core/data.tf`, `Readiness*`) allow nothing else, and a
//! terraform test pins that.
//!
//! The distribution is found through an SSM parameter the edge module writes
//! (its id, the gate function's ARN, and the polled `/status` path). The admin
//! lives in core and edge depends on core, so core cannot hand the admin the
//! distribution id without a module cycle; core names the parameter, edge
//! fills it, and this reads it per request. A missing parameter is "not
//! configured", not an error.

use aws_sdk_cloudfront::types::{CachePolicyCookieBehavior, EventType};
use aws_sdk_dynamodb::types::TableStatus;
use aws_sdk_scheduler::types::ScheduleState;

use crate::readiness::{
    ApiGatewayThrottle, CachingFacts, CookieKey, DynamoDbLimits, EdgeFacts, ProbeError,
    ReadinessProbe, ScheduleFacts, ScheduleKind, Table, WarmThroughput,
};

/// The table names, one per [`Table`].
pub struct TableNames {
    pub counters: String,
    pub prequeue: String,
    pub positions: String,
    pub tokens: String,
}

impl TableNames {
    fn name(&self, table: Table) -> &str {
        match table {
            Table::Counters => &self.counters,
            Table::PreQueue => &self.prequeue,
            Table::Positions => &self.positions,
            Table::Tokens => &self.tokens,
        }
    }
}

/// The SDK clients the probe reads through.
pub struct Clients {
    pub dynamodb: aws_sdk_dynamodb::Client,
    pub apigateway: aws_sdk_apigateway::Client,
    pub lambda: aws_sdk_lambda::Client,
    pub scheduler: aws_sdk_scheduler::Client,
    pub cloudfront: aws_sdk_cloudfront::Client,
    pub ssm: aws_sdk_ssm::Client,
}

/// What this deployment's resources are called.
pub struct Names {
    pub tables: TableNames,
    pub assign_position_function: String,
    pub open_schedule: String,
    pub controller_schedule: String,
    pub edge_parameter: String,
}

/// A live [`ReadinessProbe`] bound to one deployment.
pub struct AwsProbe {
    clients: Clients,
    names: Names,
}

impl AwsProbe {
    #[must_use]
    pub fn new(clients: Clients, names: Names) -> Self {
        Self { clients, names }
    }
}

/// What the edge module writes to the readiness parameter.
#[derive(serde::Deserialize)]
struct EdgePointer {
    distribution_id: String,
    gate_function_arn: String,
    status_path: String,
}

impl ReadinessProbe for AwsProbe {
    async fn warm_throughput(&self, table: Table) -> Result<WarmThroughput, ProbeError> {
        let out = self
            .clients
            .dynamodb
            .describe_table()
            .table_name(self.names.tables.name(table))
            .send()
            .await
            .map_err(|e| {
                ProbeError(format!(
                    "describe_table: {}",
                    aws_sdk_dynamodb::error::DisplayErrorContext(&e)
                ))
            })?;
        let warm = out.table().and_then(|t| t.warm_throughput());
        Ok(WarmThroughput {
            write_units: warm.and_then(
                aws_sdk_dynamodb::types::TableWarmThroughputDescription::write_units_per_second,
            ),
            read_units: warm.and_then(
                aws_sdk_dynamodb::types::TableWarmThroughputDescription::read_units_per_second,
            ),
            warming: warm
                .and_then(|w| w.status())
                .is_some_and(|s| *s == TableStatus::Updating),
        })
    }

    async fn dynamodb_limits(&self) -> Result<DynamoDbLimits, ProbeError> {
        let out = self
            .clients
            .dynamodb
            .describe_limits()
            .send()
            .await
            .map_err(|e| {
                ProbeError(format!(
                    "describe_limits: {}",
                    aws_sdk_dynamodb::error::DisplayErrorContext(&e)
                ))
            })?;
        Ok(DynamoDbLimits {
            table_max_write: out.table_max_write_capacity_units(),
            table_max_read: out.table_max_read_capacity_units(),
            account_max_write: out.account_max_write_capacity_units(),
            account_max_read: out.account_max_read_capacity_units(),
        })
    }

    async fn api_gateway_throttle(&self) -> Result<ApiGatewayThrottle, ProbeError> {
        let out = self
            .clients
            .apigateway
            .get_account()
            .send()
            .await
            .map_err(|e| {
                ProbeError(format!(
                    "get_account: {}",
                    aws_sdk_apigateway::error::DisplayErrorContext(&e)
                ))
            })?;
        let throttle = out.throttle_settings();
        Ok(ApiGatewayThrottle {
            rate_limit: throttle.map(aws_sdk_apigateway::types::ThrottleSettings::rate_limit),
            burst_limit: throttle.map(aws_sdk_apigateway::types::ThrottleSettings::burst_limit),
        })
    }

    async fn reserved_concurrency(&self) -> Result<Option<i32>, ProbeError> {
        let out = self
            .clients
            .lambda
            .get_function_concurrency()
            .function_name(&self.names.assign_position_function)
            .send()
            .await
            .map_err(|e| {
                ProbeError(format!(
                    "get_function_concurrency: {}",
                    aws_sdk_lambda::error::DisplayErrorContext(&e)
                ))
            })?;
        Ok(out.reserved_concurrent_executions())
    }

    async fn schedule(&self, kind: ScheduleKind) -> Result<Option<ScheduleFacts>, ProbeError> {
        let name = match kind {
            ScheduleKind::Open => &self.names.open_schedule,
            ScheduleKind::Controller => &self.names.controller_schedule,
        };
        let out = match self.clients.scheduler.get_schedule().name(name).send().await {
            Ok(out) => out,
            Err(e)
                if e.as_service_error()
                    .is_some_and(aws_sdk_scheduler::operation::get_schedule::GetScheduleError::is_resource_not_found_exception) =>
            {
                return Ok(None);
            }
            Err(e) => {
                return Err(ProbeError(format!(
                    "get_schedule {name}: {}",
                    aws_sdk_scheduler::error::DisplayErrorContext(&e)
                )));
            }
        };
        Ok(Some(ScheduleFacts {
            enabled: out.state() == Some(&ScheduleState::Enabled),
            expression: out.schedule_expression().unwrap_or_default().to_owned(),
            timezone: out
                .schedule_expression_timezone()
                .unwrap_or("UTC")
                .to_owned(),
        }))
    }

    async fn edge_facts(&self) -> Result<Option<EdgeFacts>, ProbeError> {
        let Some(pointer) = self.edge_pointer().await? else {
            return Ok(None);
        };

        let out = self
            .clients
            .cloudfront
            .get_distribution_config()
            .id(&pointer.distribution_id)
            .send()
            .await
            .map_err(|e| {
                ProbeError(format!(
                    "get_distribution_config {}: {}",
                    pointer.distribution_id,
                    aws_sdk_cloudfront::error::DisplayErrorContext(&e)
                ))
            })?;
        let config = out
            .distribution_config()
            .ok_or_else(|| ProbeError("get_distribution_config: no configuration".to_owned()))?;

        let is_gate = |arn: &str| arn == pointer.gate_function_arn;

        let gate_on_default = config
            .default_cache_behavior()
            .and_then(|b| b.function_associations())
            .is_some_and(|fa| {
                fa.items().iter().any(|a| {
                    *a.event_type() == EventType::ViewerRequest && is_gate(a.function_arn())
                })
            });

        let behaviours = config
            .cache_behaviors()
            .map(aws_sdk_cloudfront::types::CacheBehaviors::items)
            .unwrap_or_default();
        let mut gate_elsewhere = Vec::new();
        let mut status = None;
        for b in behaviours {
            let carries_gate = b
                .function_associations()
                .is_some_and(|fa| fa.items().iter().any(|a| is_gate(a.function_arn())));
            if carries_gate {
                gate_elsewhere.push(b.path_pattern().to_owned());
            }
            if b.path_pattern() == pointer.status_path {
                status = Some(b);
            }
        }

        let status_caching = match status {
            None => None,
            Some(b) => {
                // The edge module always attaches a cache policy. A behaviour
                // on CloudFront's deprecated legacy cache settings was edited
                // outside Terraform, and is reported rather than guessed at.
                let id = b.cache_policy_id().ok_or_else(|| {
                    ProbeError(format!(
                        "{} uses legacy cache settings, not a cache policy",
                        pointer.status_path
                    ))
                })?;
                Some(self.cache_policy_facts(id).await?)
            }
        };

        Ok(Some(EdgeFacts {
            status_path: pointer.status_path,
            status_caching,
            gate_on_default,
            gate_elsewhere,
        }))
    }
}

impl AwsProbe {
    /// Reads where the distribution is. `None` while the parameter does not
    /// exist yet.
    async fn edge_pointer(&self) -> Result<Option<EdgePointer>, ProbeError> {
        let out = match self
            .clients
            .ssm
            .get_parameter()
            .name(&self.names.edge_parameter)
            .send()
            .await
        {
            Ok(out) => out,
            Err(e)
                if e.as_service_error()
                    .is_some_and(aws_sdk_ssm::operation::get_parameter::GetParameterError::is_parameter_not_found) =>
            {
                return Ok(None);
            }
            Err(e) => {
                return Err(ProbeError(format!(
                    "get_parameter {}: {}",
                    self.names.edge_parameter,
                    aws_sdk_ssm::error::DisplayErrorContext(&e)
                )));
            }
        };
        let value = out
            .parameter()
            .and_then(|p| p.value())
            .ok_or_else(|| ProbeError("the edge readiness parameter has no value".to_owned()))?;
        serde_json::from_str(value)
            .map(Some)
            .map_err(|e| ProbeError(format!("the edge readiness parameter is malformed: {e}")))
    }

    async fn cache_policy_facts(&self, id: &str) -> Result<CachingFacts, ProbeError> {
        let out = self
            .clients
            .cloudfront
            .get_cache_policy()
            .id(id)
            .send()
            .await
            .map_err(|e| {
                ProbeError(format!(
                    "get_cache_policy {id}: {}",
                    aws_sdk_cloudfront::error::DisplayErrorContext(&e)
                ))
            })?;
        let config = out
            .cache_policy()
            .and_then(|p| p.cache_policy_config())
            .ok_or_else(|| ProbeError(format!("get_cache_policy {id}: no configuration")))?;
        let cookies = config
            .parameters_in_cache_key_and_forwarded_to_origin()
            .and_then(|p| p.cookies_config())
            .map(aws_sdk_cloudfront::types::CachePolicyCookiesConfig::cookie_behavior);
        Ok(CachingFacts {
            min_ttl: config.min_ttl(),
            cookies: match cookies {
                Some(b) if *b != CachePolicyCookieBehavior::None => {
                    CookieKey::Included(b.as_str().to_owned())
                }
                Some(_) | None => CookieKey::None,
            },
        })
    }
}
