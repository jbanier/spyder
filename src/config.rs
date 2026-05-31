use anyhow::{Context, Result};
use std::env;
use std::time::Duration;

/// Main application configuration
#[derive(Clone, Debug)]
pub struct SpyderConfig {
    pub database: DatabaseConfig,
    pub crawler: CrawlerConfig,
    pub frontend: FrontendConfig,
}

/// Database connection configuration
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub url: String,
    pub pool_size: u32,
}

/// Crawler behavior configuration
#[derive(Clone, Debug)]
pub struct CrawlerConfig {
    pub concurrency: usize,
    pub timeout_seconds: u64,
    pub max_retries: i32,
    pub user_agent: String,
}

/// Frontend server configuration
#[derive(Clone, Debug)]
pub struct FrontendConfig {
    pub pool_size: u32,
    pub cache_ttl_seconds: u64,
    pub cache_cold_wait_ms: u64,
    pub cache_warm_routes: String,
    pub cache_slow_route_ms: u64,
    pub dashboard_deep: bool,
}

// Default values
pub const DEFAULT_CRAWLER_CONCURRENCY: usize = 4;
pub const DEFAULT_CRAWLER_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_CRAWLER_MAX_RETRIES: i32 = 5;
pub const DEFAULT_USER_AGENT: &str = "Spyder/0.1";

pub const DEFAULT_FRONTEND_POOL_SIZE: u32 = 16;
pub const DEFAULT_FRONTEND_CACHE_TTL_SECONDS: u64 = 30;
pub const DEFAULT_FRONTEND_CACHE_COLD_WAIT_MS: u64 = 0;
pub const DEFAULT_FRONTEND_CACHE_WARM_ROUTES: &str = "/,/analytics";
pub const DEFAULT_FRONTEND_CACHE_SLOW_ROUTE_MS: u64 = 5000;

pub const DEFAULT_TOP_SITE_LIMIT: i64 = 25;
pub const DEFAULT_PAGE_LIMIT: i64 = 50;
pub const MAX_PAGE_LIMIT: i64 = 200;
pub const DEFAULT_RELATIONSHIP_GRAPH_LIMIT: i64 = 80;
pub const MIN_RELATIONSHIP_GRAPH_LIMIT: i64 = 10;
pub const MAX_RELATIONSHIP_GRAPH_DEPTH: i64 = 4;
pub const DEFAULT_RELATIONSHIP_GRAPH_DEPTH: i64 = 2;

impl SpyderConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let database_url = env::var("DATABASE_URL")
            .context("DATABASE_URL must be set")?;

        // Validate database URL
        if !database_url.starts_with("postgres://") && !database_url.starts_with("postgresql://") {
            anyhow::bail!("DATABASE_URL must be a PostgreSQL connection string (postgres://...)");
        }

        Ok(SpyderConfig {
            database: DatabaseConfig {
                url: database_url,
                pool_size: env_u32("SPYDER_DB_POOL_SIZE", DEFAULT_FRONTEND_POOL_SIZE),
            },
            crawler: CrawlerConfig {
                concurrency: env_usize("SPYDER_WORK_CONCURRENCY", DEFAULT_CRAWLER_CONCURRENCY),
                timeout_seconds: env_u64("SPYDER_TIMEOUT_SECONDS", DEFAULT_CRAWLER_TIMEOUT_SECONDS),
                max_retries: env_i32("SPYDER_MAX_RETRIES", DEFAULT_CRAWLER_MAX_RETRIES),
                user_agent: env::var("SPYDER_USER_AGENT")
                    .unwrap_or_else(|_| DEFAULT_USER_AGENT.to_string()),
            },
            frontend: FrontendConfig {
                pool_size: env_u32("SPYDER_FRONTEND_DB_POOL_SIZE", DEFAULT_FRONTEND_POOL_SIZE),
                cache_ttl_seconds: env_u64(
                    "SPYDER_FRONTEND_CACHE_TTL_SECONDS",
                    DEFAULT_FRONTEND_CACHE_TTL_SECONDS,
                ),
                cache_cold_wait_ms: env_u64(
                    "SPYDER_FRONTEND_CACHE_COLD_WAIT_MS",
                    DEFAULT_FRONTEND_CACHE_COLD_WAIT_MS,
                ),
                cache_warm_routes: env::var("SPYDER_FRONTEND_CACHE_WARM_ROUTES")
                    .unwrap_or_else(|_| DEFAULT_FRONTEND_CACHE_WARM_ROUTES.to_string()),
                cache_slow_route_ms: env_u64(
                    "SPYDER_FRONTEND_CACHE_SLOW_ROUTE_MS",
                    DEFAULT_FRONTEND_CACHE_SLOW_ROUTE_MS,
                ),
                dashboard_deep: env::var("SPYDER_DASHBOARD_DEEP")
                    .ok()
                    .and_then(|v| v.parse::<i32>().ok())
                    .map(|v| v > 0)
                    .unwrap_or(false),
            },
        })
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<()> {
        if self.database.pool_size == 0 {
            anyhow::bail!("Database pool size must be greater than 0");
        }

        if self.crawler.concurrency == 0 {
            anyhow::bail!("Crawler concurrency must be greater than 0");
        }

        if self.crawler.timeout_seconds == 0 {
            anyhow::bail!("Crawler timeout must be greater than 0");
        }

        if self.frontend.pool_size == 0 {
            anyhow::bail!("Frontend pool size must be greater than 0");
        }

        Ok(())
    }

    /// Get cache TTL as Duration
    pub fn cache_ttl(&self) -> Duration {
        Duration::from_secs(self.frontend.cache_ttl_seconds)
    }

    /// Get cache cold wait as Duration
    pub fn cache_cold_wait(&self) -> Duration {
        Duration::from_millis(self.frontend.cache_cold_wait_ms)
    }

    /// Get cache slow route threshold as Duration
    pub fn cache_slow_route_threshold(&self) -> Duration {
        Duration::from_millis(self.frontend.cache_slow_route_ms)
    }
}

// Helper functions for parsing environment variables

fn env_u32(key: &str, default_value: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .unwrap_or(default_value)
}

fn env_u64(key: &str, default_value: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default_value)
}

fn env_i32(key: &str, default_value: i32) -> i32 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<i32>().ok())
        .unwrap_or(default_value)
}

fn env_usize(key: &str, default_value: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        env::set_var("DATABASE_URL", "postgres://localhost/spyder");

        let config = SpyderConfig::from_env().expect("should load config");
        assert!(config.validate().is_ok());

        assert_eq!(config.database.url, "postgres://localhost/spyder");
        assert_eq!(config.crawler.concurrency, DEFAULT_CRAWLER_CONCURRENCY);
        assert_eq!(config.frontend.pool_size, DEFAULT_FRONTEND_POOL_SIZE);
    }

    #[test]
    fn test_invalid_database_url() {
        env::set_var("DATABASE_URL", "mysql://localhost/spyder");

        let result = SpyderConfig::from_env();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("PostgreSQL"));
    }

    #[test]
    fn test_custom_values() {
        env::set_var("DATABASE_URL", "postgres://localhost/test");
        env::set_var("SPYDER_WORK_CONCURRENCY", "8");
        env::set_var("SPYDER_TIMEOUT_SECONDS", "60");

        let config = SpyderConfig::from_env().expect("should load config");

        assert_eq!(config.crawler.concurrency, 8);
        assert_eq!(config.crawler.timeout_seconds, 60);
    }
}
