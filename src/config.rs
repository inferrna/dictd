use std::path::MAIN_SEPARATOR;
use serde::Deserialize;



#[derive(Debug, Deserialize)]
pub(crate) struct FallbackConfig {
    db: String,
    host: String,
    port: u32,
}
#[derive(Debug, Deserialize)]
pub(crate) struct DatabaseConfig {
    name: Option<String>,
    short_name: Option<String>,
    path: String,
    fallback: Option<FallbackConfig>,
}
impl DatabaseConfig {
    pub fn name(&self) -> String {
        self.name.as_ref().cloned().unwrap_or_else(|| self.short_name()).clone()
    }
    pub fn short_name(&self) -> String {
        self.short_name.as_ref().cloned()
            .unwrap_or_else(||self.path.split(MAIN_SEPARATOR)
            .last()
            .unwrap()
            .split(".")
            .next()
            .unwrap()
            .to_string())
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn fallback(&self) -> Option<&FallbackConfig> {
        self.fallback.as_ref()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ServerConfig {
    host: String,
    port: u32,
}


impl ServerConfig {
    fn host(&self) -> &str {
        &self.host
    }
    fn port(&self) -> u32 {
        self.port
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    server: ServerConfig,
    databases: Vec<DatabaseConfig>,
}

impl Config {
    pub(crate) fn host(&self) -> &str {
        self.server.host()
    }
    pub(crate) fn port(&self) -> u32 {
        self.server.port()
    }
    pub fn databases(&self) -> &Vec<DatabaseConfig> {
        &self.databases
    }
}

