use std::{
    fs, io,
    path::{Path, PathBuf},
};

use clap::ValueEnum;
use serde_json::{Value, json};
use toml_edit::{DocumentMut, Item, Table, value};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Client {
    All,
    Codex,
    Claude,
    Opencode,
    Pi,
}

#[derive(Clone, Debug)]
pub struct ConfigureOptions {
    pub clients: Vec<Client>,
    pub home: PathBuf,
    pub codex_home: Option<PathBuf>,
    pub proxy_url: String,
    pub model: String,
    pub direct_mode: bool,
    pub dry_run: bool,
}

pub fn configure(options: &ConfigureOptions) -> Result<(), String> {
    let all_clients = options.clients.is_empty() || options.clients.contains(&Client::All);
    if !all_clients && options.clients.contains(&Client::Claude) {
        return Err(
            "Claude Code was not configured: Kaiion exposes OpenAI Responses, while Claude Code requires Anthropic Messages; add an adapter before routing it".to_string(),
        );
    }
    let clients = if all_clients {
        eprintln!(
            "skipping Claude Code: Kaiion exposes OpenAI Responses, while Claude Code requires Anthropic Messages"
        );
        vec![Client::Codex, Client::Opencode, Client::Pi]
    } else {
        options.clients.clone()
    };
    for client in clients {
        match client {
            Client::All => unreachable!(),
            Client::Codex => configure_codex(options)?,
            Client::Claude => unreachable!(),
            Client::Opencode => configure_opencode(options)?,
            Client::Pi => configure_pi(options)?,
        }
    }
    Ok(())
}

fn configure_codex(options: &ConfigureOptions) -> Result<(), String> {
    let path = options
        .codex_home
        .clone()
        .unwrap_or_else(|| options.home.join(".codex"))
        .join("config.toml");
    let mut document = if path.exists() {
        let content = fs::read_to_string(&path).map_err(io_error("read Codex config"))?;
        content
            .parse::<DocumentMut>()
            .map_err(|error| format!("could not parse Codex config {}: {error}", path.display()))?
    } else {
        DocumentMut::new()
    };
    document["model_provider"] = value("kaiion");
    let providers = document
        .entry("model_providers")
        .or_insert(Item::Table(Table::new()))
        .as_table_mut()
        .ok_or_else(|| "Codex model_providers must be a table".to_string())?;
    let provider = providers
        .entry("kaiion")
        .or_insert(Item::Table(Table::new()))
        .as_table_mut()
        .ok_or_else(|| "Codex kaiion provider must be a table".to_string())?;
    provider["name"] = value("Kaiion");
    provider["base_url"] = value(&options.proxy_url);
    provider["env_key"] = value("OPENAI_API_KEY");
    provider["wire_api"] = value("responses");
    provider["supports_websockets"] = value(false);
    provider["stream_idle_timeout_ms"] = value(300_000_i64);
    write_text(&path, document.to_string(), options.dry_run)?;
    Ok(())
}

fn configure_opencode(options: &ConfigureOptions) -> Result<(), String> {
    let json_path = options.home.join(".config/opencode/opencode.json");
    let jsonc_path = options.home.join(".config/opencode/opencode.jsonc");
    let path = if json_path.exists() {
        json_path
    } else if jsonc_path.exists() {
        jsonc_path
    } else {
        json_path
    };
    let mut document = read_json(&path)?;
    let root = document
        .as_object_mut()
        .ok_or_else(|| format!("OpenCode config {} must contain an object", path.display()))?;
    {
        let provider = root
            .entry("provider")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| "OpenCode provider must be an object".to_string())?;
        let mut kaiion = provider.get("kaiion").cloned().unwrap_or_else(|| json!({}));
        let kaiion_object = kaiion
            .as_object_mut()
            .ok_or_else(|| "OpenCode kaiion provider must be an object".to_string())?;
        kaiion_object.insert("npm".to_string(), json!("@ai-sdk/openai"));
        kaiion_object.insert("name".to_string(), json!("Kaiion"));
        let provider_options = kaiion_object
            .entry("options")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| "OpenCode kaiion options must be an object".to_string())?;
        provider_options.insert("baseURL".to_string(), json!(options.proxy_url));
        provider_options.insert("apiKey".to_string(), json!("{env:OPENAI_API_KEY}"));
        let headers = provider_options
            .entry("headers")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| "OpenCode kaiion headers must be an object".to_string())?;
        headers.insert(
            "x-kaiion-mode".to_string(),
            json!(if options.direct_mode {
                "direct"
            } else {
                "batch"
            }),
        );
        let models = kaiion_object
            .entry("models")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| "OpenCode kaiion models must be an object".to_string())?;
        models
            .entry(options.model.clone())
            .or_insert_with(|| json!({"name": options.model}));
        provider.insert("kaiion".to_string(), kaiion);
    }
    root.insert(
        "model".to_string(),
        Value::String(format!("kaiion/{}", options.model)),
    );
    write_json(&path, &document, options.dry_run)
}

fn configure_pi(options: &ConfigureOptions) -> Result<(), String> {
    let path = options.home.join(".pi/agent/models.json");
    let mut document = read_json(&path)?;
    let root = document
        .as_object_mut()
        .ok_or_else(|| format!("Pi config {} must contain an object", path.display()))?;
    let providers = root
        .entry("providers")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| "Pi providers must be an object".to_string())?;
    let mut provider = providers
        .get("kaiion")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let provider_object = provider
        .as_object_mut()
        .ok_or_else(|| "Pi kaiion provider must be an object".to_string())?;
    provider_object.insert("baseUrl".to_string(), json!(options.proxy_url));
    provider_object.insert("api".to_string(), json!("openai-responses"));
    provider_object.insert("apiKey".to_string(), json!("$OPENAI_API_KEY"));
    let models = provider_object
        .entry("models")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .ok_or_else(|| "Pi kaiion models must be an array".to_string())?;
    if let Some(model) = models
        .iter_mut()
        .find(|model| model.get("id").and_then(Value::as_str) == Some(options.model.as_str()))
    {
        model["name"] = json!(options.model);
    } else {
        models.push(json!({"id": options.model, "name": options.model}));
    }
    let headers = provider_object
        .entry("headers")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| "Pi kaiion headers must be an object".to_string())?;
    headers.insert(
        "x-kaiion-mode".to_string(),
        json!(if options.direct_mode {
            "direct"
        } else {
            "batch"
        }),
    );
    providers.insert("kaiion".to_string(), provider);
    write_json(&path, &document, options.dry_run)
}

fn read_json(path: &Path) -> Result<Value, String> {
    if !path.exists() {
        return Ok(json!({}));
    }
    let content = fs::read_to_string(path).map_err(io_error("read JSON config"))?;
    match serde_json::from_str(&content) {
        Ok(value) => Ok(value),
        Err(json_error) => json5::from_str(&content).map_err(|json5_error| {
            format!(
                "could not parse JSON config {}: {json_error}; JSON5 parser: {json5_error}",
                path.display()
            )
        }),
    }
}

fn write_json(path: &Path, document: &Value, dry_run: bool) -> Result<(), String> {
    let content = serde_json::to_string_pretty(document)
        .map_err(|error| format!("could not serialize {}: {error}", path.display()))?;
    write_text(path, format!("{content}\n"), dry_run)
}

fn write_text(path: &Path, content: String, dry_run: bool) -> Result<(), String> {
    if dry_run {
        println!("would update {}", path.display());
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_error("create configuration directory"))?;
    }
    crate::atomic_file::write(path, content.as_bytes()).map_err(io_error("write configuration"))
}

fn io_error(action: &'static str) -> impl FnOnce(io::Error) -> String {
    move |error| format!("could not {action}: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn configures_codex_without_removing_existing_settings() {
        let home = TempDir::new().unwrap();
        let path = home.path().join(".codex/config.toml");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            "model = 'gpt-test'\n\n[profiles.work]\nmodel = 'other'\n",
        )
        .unwrap();
        configure_codex(&ConfigureOptions {
            clients: vec![Client::Codex],
            home: home.path().to_path_buf(),
            codex_home: None,
            proxy_url: "http://127.0.0.1:8787/v1".to_string(),
            model: "gpt-test".to_string(),
            direct_mode: false,
            dry_run: false,
        })
        .unwrap();
        let result = fs::read_to_string(path).unwrap();
        assert!(result.contains("model = 'gpt-test'"));
        assert!(result.contains("model_provider = \"kaiion\""));
        assert!(result.contains("base_url = \"http://127.0.0.1:8787/v1\""));
    }

    #[test]
    fn configures_json_clients_and_preserves_unrelated_keys() {
        let home = TempDir::new().unwrap();
        let path = home.path().join(".pi/agent/models.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"{"providers":{"other":{"baseUrl":"http://other"}},"custom":true}"#,
        )
        .unwrap();
        configure_pi(&ConfigureOptions {
            clients: vec![Client::Pi],
            home: home.path().to_path_buf(),
            codex_home: None,
            proxy_url: "http://127.0.0.1:8787/v1".to_string(),
            model: "gpt-test".to_string(),
            direct_mode: true,
            dry_run: false,
        })
        .unwrap();
        let result: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(result["custom"], true);
        assert_eq!(result["providers"]["other"]["baseUrl"], "http://other");
        assert_eq!(result["providers"]["kaiion"]["api"], "openai-responses");
        assert_eq!(
            result["providers"]["kaiion"]["headers"]["x-kaiion-mode"],
            "direct"
        );
    }
}
