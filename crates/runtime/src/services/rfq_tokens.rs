use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use reqwest::Client;
use tracing::info;
use tycho_simulation::tycho_common::{
    models::{token::Token, Chain},
    Bytes,
};

use crate::models::rfq::bebop::BebopResponse;
use crate::models::rfq::hashflow::read_hashflow_csv;
use crate::models::rfq::liquorice::TokenLiquorice;
use crate::models::tokens::TokenStore;
use crate::services::stream_builder::RFQTokenStores;

pub struct RfqTokenStoreConfig<'a> {
    pub tokens: Arc<TokenStore>,
    pub chain: Chain,
    pub token_refresh_timeout: Duration,
    pub protocols: &'a [String],
    pub bebop_url: &'a str,
    pub hashflow_filename: &'a str,
    pub liquorice_url: Option<&'a str>,
    pub liquorice_user: &'a str,
    pub liquorice_key: &'a str,
}

pub async fn load_rfq_token_stores(
    config: RfqTokenStoreConfig<'_>,
) -> anyhow::Result<RFQTokenStores> {
    let bebop = if rfq_protocol_enabled(config.protocols, "rfq:bebop") {
        load_bebop_token_store(config.bebop_url, config.chain, config.token_refresh_timeout).await?
    } else {
        new_local_token_store(HashMap::new(), config.chain, config.token_refresh_timeout)
    };
    let hashflow = if rfq_protocol_enabled(config.protocols, "rfq:hashflow") {
        load_hashflow_token_store(
            config.hashflow_filename,
            config.chain,
            config.token_refresh_timeout,
        )?
    } else {
        new_local_token_store(HashMap::new(), config.chain, config.token_refresh_timeout)
    };
    let liquorice = if rfq_protocol_enabled(config.protocols, "rfq:liquorice") {
        let liquorice_url = config.liquorice_url.ok_or_else(|| {
            anyhow::anyhow!(
                "Liquorice RFQ enabled for {} without liquorice_url",
                config.chain
            )
        })?;
        load_liquorice_token_store(
            liquorice_url,
            config.chain,
            config.liquorice_user,
            config.liquorice_key,
            config.token_refresh_timeout,
        )
        .await?
    } else {
        new_local_token_store(HashMap::new(), config.chain, config.token_refresh_timeout)
    };

    Ok(RFQTokenStores {
        tokens: config.tokens,
        bebop,
        hashflow,
        liquorice,
    })
}

async fn load_bebop_token_store(
    bebop_url: &str,
    chain: Chain,
    token_refresh_timeout: Duration,
) -> anyhow::Result<Arc<TokenStore>> {
    let client = build_rfq_token_client(token_refresh_timeout)?;
    let bebop_tokens = load_bebop_tokens(&client, bebop_url, chain, token_refresh_timeout).await?;
    info!("all bebop tokens: {:?}", bebop_tokens);
    Ok(new_local_token_store(
        bebop_tokens,
        chain,
        token_refresh_timeout,
    ))
}

fn build_rfq_token_client(request_timeout: Duration) -> anyhow::Result<Client> {
    Client::builder()
        .connect_timeout(request_timeout)
        .timeout(request_timeout)
        .no_proxy()
        .build()
        .map_err(|error| anyhow::anyhow!("Failed to build RFQ token HTTP client: {}", error))
}

async fn load_bebop_tokens(
    client: &Client,
    bebop_url: &str,
    chain: Chain,
    request_timeout: Duration,
) -> anyhow::Result<HashMap<Bytes, Token>> {
    let response: BebopResponse =
        run_rfq_token_request("Bebop", bebop_url, request_timeout, async {
            client
                .get(bebop_url)
                .query(&[
                    ("active_only", "true"),
                    ("gasless", "false"),
                    ("expiry_type", "standard"),
                ])
                .header("accept", "application/json")
                .timeout(request_timeout)
                .send()
                .await?
                .json()
                .await
        })
        .await?;

    response
        .tokens
        .into_values()
        .filter_map(|token| match token.to_tycho_token(chain) {
            Ok(Some(new)) => Some(Ok((new.address.clone(), new))),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        })
        .collect::<Result<_, _>>()
        .map_err(|error| anyhow::anyhow!("Failed to parse Bebop token: {}", error))
}

async fn run_rfq_token_request<T, F>(
    provider: &str,
    url: &str,
    request_timeout: Duration,
    request: F,
) -> anyhow::Result<T>
where
    F: Future<Output = Result<T, reqwest::Error>>,
{
    tokio::pin!(request);
    tokio::select! {
        result = &mut request => {
            result.map_err(|error| rfq_token_request_error(provider, url, request_timeout, error))
        }
        () = tokio::time::sleep(request_timeout) => {
            Err(rfq_token_request_timeout(provider, url, request_timeout))
        }
    }
}

fn load_hashflow_token_store(
    hashflow_filename: &str,
    chain: Chain,
    token_refresh_timeout: Duration,
) -> anyhow::Result<Arc<TokenStore>> {
    let hashflow_tokens = read_hashflow_csv(hashflow_filename, chain)
        .map_err(|error| anyhow::anyhow!("Failed to read hashflow CSV: {}", error))?;
    info!("all_hashflow_tokens: {:?}", hashflow_tokens);
    Ok(new_local_token_store(
        hashflow_tokens,
        chain,
        token_refresh_timeout,
    ))
}

async fn load_liquorice_token_store(
    liquorice_url: &str,
    chain: Chain,
    solver: &str,
    authorization: &str,
    token_refresh_timeout: Duration,
) -> anyhow::Result<Arc<TokenStore>> {
    let client = build_rfq_token_client(token_refresh_timeout)?;
    let liquorice_tokens = load_liquorice_tokens(
        &client,
        liquorice_url,
        chain,
        solver,
        authorization,
        token_refresh_timeout,
    )
    .await?;
    info!("all_liquorice_tokens: {:?}", liquorice_tokens);
    Ok(new_local_token_store(
        liquorice_tokens,
        chain,
        token_refresh_timeout,
    ))
}

async fn load_liquorice_tokens(
    client: &Client,
    liquorice_url: &str,
    chain: Chain,
    solver: &str,
    authorization: &str,
    request_timeout: Duration,
) -> anyhow::Result<HashMap<Bytes, Token>> {
    let chain_id = chain.id().to_string();
    let response: Vec<TokenLiquorice> =
        run_rfq_token_request("Liquorice", liquorice_url, request_timeout, async {
            client
                .get(liquorice_url)
                .query(&[("chainId", chain_id)])
                .header("accept", "application/json")
                .header("solver", solver)
                .header("authorization", authorization)
                .timeout(request_timeout)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
        })
        .await?;

    response
        .into_iter()
        .map(|token| {
            token
                .to_tycho_token(chain)
                .map(|new| (new.address.clone(), new))
        })
        .collect::<Result<_, _>>()
        .map_err(|error| anyhow::anyhow!("Failed to parse Liquorice token: {}", error))
}

fn rfq_token_request_timeout(provider: &str, url: &str, timeout: Duration) -> anyhow::Error {
    anyhow::anyhow!(
        "{} RFQ token request to {} timed out after {} ms",
        provider,
        url,
        timeout.as_millis()
    )
}

fn rfq_token_request_error(
    provider: &str,
    url: &str,
    timeout: Duration,
    error: reqwest::Error,
) -> anyhow::Error {
    if error.is_timeout() {
        anyhow::anyhow!(
            "{} RFQ token request to {} timed out after {} ms",
            provider,
            url,
            timeout.as_millis()
        )
    } else {
        anyhow::anyhow!(
            "{} RFQ token request to {} failed: {}",
            provider,
            url,
            error
        )
    }
}

fn rfq_protocol_enabled(protocols: &[String], protocol: &str) -> bool {
    protocols.iter().any(|configured| configured == protocol)
}

fn new_local_token_store(
    tokens: HashMap<Bytes, Token>,
    chain: Chain,
    token_refresh_timeout: Duration,
) -> Arc<TokenStore> {
    Arc::new(TokenStore::local_only(tokens, chain, token_refresh_timeout))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use anyhow::Result;
    use tokio::{io::AsyncReadExt, net::TcpListener, task::JoinHandle};
    use tycho_simulation::tycho_common::models::Chain;

    use super::{load_bebop_token_store, load_liquorice_token_store};

    async fn spawn_stalling_provider() -> Result<(String, JoinHandle<()>)> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            if let Ok((mut stream, _peer)) = listener.accept().await {
                let mut buf = [0; 1024];
                let _ = stream.read(&mut buf).await;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        Ok((format!("http://{address}/tokens"), handle))
    }

    #[tokio::test]
    async fn bebop_token_bootstrap_times_out_stalled_provider() -> Result<()> {
        let (url, handle) = spawn_stalling_provider().await?;
        let started_at = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            load_bebop_token_store(&url, Chain::Base, Duration::from_millis(50)),
        )
        .await;
        handle.abort();

        let error = match result {
            Ok(Ok(_)) => anyhow::bail!("stalled Bebop provider should fail"),
            Ok(Err(error)) => error,
            Err(_) => {
                anyhow::bail!("Bebop RFQ token request should honor the configured timeout");
            }
        };

        assert!(
            started_at.elapsed() < Duration::from_secs(2),
            "request ignored timeout: {error}"
        );
        assert!(error.to_string().contains("timed out"));
        Ok(())
    }

    #[tokio::test]
    async fn liquorice_token_bootstrap_times_out_stalled_provider() -> Result<()> {
        let (url, handle) = spawn_stalling_provider().await?;
        let started_at = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            load_liquorice_token_store(
                &url,
                Chain::Base,
                "solver",
                "key",
                Duration::from_millis(50),
            ),
        )
        .await;
        handle.abort();

        let error = match result {
            Ok(Ok(_)) => anyhow::bail!("stalled Liquorice provider should fail"),
            Ok(Err(error)) => error,
            Err(_) => {
                anyhow::bail!("Liquorice RFQ token request should honor the configured timeout");
            }
        };

        assert!(
            started_at.elapsed() < Duration::from_secs(2),
            "request ignored timeout: {error}"
        );
        assert!(error.to_string().contains("timed out"));
        Ok(())
    }
}
