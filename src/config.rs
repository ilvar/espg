use std::env;
use std::str::FromStr;
use tokio_postgres::Config as PostgresConfig;

pub struct Config {
    pub postgres: PostgresConfig,
    pub pool_size: usize,
    pub http_port: u16,
    pub passthrough_url: Option<String>,
    pub passthrough_indices: Vec<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let postgres = build_postgres_config()?;
        let pool_size = parse_env::<usize>("PGPOOL_SIZE", 10)?;
        let http_port = parse_env::<u16>("PORT", 3000)?;
        let passthrough_url = non_empty_env("PASSTHROUGH_URL");
        let passthrough_indices = env::var("PASSTHROUGH_INDICES")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        Ok(Self {
            postgres,
            pool_size,
            http_port,
            passthrough_url,
            passthrough_indices,
        })
    }
}

fn build_postgres_config() -> Result<PostgresConfig, String> {
    if let Some(database_url) = non_empty_env("DATABASE_URL") {
        return PostgresConfig::from_str(&database_url)
            .map_err(|error| format!("invalid DATABASE_URL: {error}"));
    }

    let host = env_or("PGHOST", "localhost");
    let port = parse_env::<u16>("PGPORT", 5432)?;
    let user = env_or("PGUSER", "postgres");
    let password = env::var("PGPASSWORD").unwrap_or_default();
    let database = env_or("PGDATABASE", "postgres");

    let mut config = PostgresConfig::new();
    config.host(&host);
    config.port(port);
    config.user(&user);
    if !password.is_empty() {
        config.password(password);
    }
    config.dbname(&database);
    Ok(config)
}

fn parse_env<T>(key: &str, fallback: T) -> Result<T, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let Some(raw) = non_empty_env(key) else {
        return Ok(fallback);
    };
    raw.parse::<T>()
        .map_err(|error| format!("invalid {key}: {error}"))
}

fn env_or(key: &str, fallback: &str) -> String {
    non_empty_env(key).unwrap_or_else(|| fallback.to_owned())
}

fn non_empty_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}
