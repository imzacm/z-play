use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::random_files::{ScanResult, reduce_scan_result};

pub async fn random_file_with_timeout(
    client: &ImmichClient,
    roots: &[PathBuf],
    timeout: Duration,
) -> Option<PathBuf> {
    if roots.is_empty() {
        return None;
    }

    let deadline = std::time::Instant::now() + timeout;

    let mut join_set = tokio::task::JoinSet::new();
    for root in roots {
        let client = client.clone();
        let root = Utf8Path::from_path(root).expect("Invalid path").to_path_buf();
        join_set.spawn(async move { search_root(&client, &root, deadline).await });
    }

    let mut search_result = ScanResult::default();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(result)) => search_result = reduce_scan_result(search_result, result),
            Ok(Err(error)) => eprintln!("Error searching roots: {error:?}"),
            Err(error) => eprintln!("Search roots task panicked: {error:?}"),
        }
    }

    search_result.selected.map(Into::into)
}

async fn search_root(
    client: &ImmichClient,
    root: &Utf8Path,
    deadline: std::time::Instant,
) -> Result<ScanResult<Utf8PathBuf>, reqwest::Error> {
    let base = SearchRequest { original_path: root.as_str() };
    let mut request = SearchPageRequest { base, page: 1, size: 1000 };

    let mut search_result = ScanResult::default();
    loop {
        if std::time::Instant::now() >= deadline {
            break;
        }

        let result = {
            static PERMITS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(3));

            let _permit = match tokio::time::timeout_at(deadline.into(), PERMITS.acquire()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => unreachable!(),
                // Timeout
                Err(_) => break,
            };

            tokio::time::timeout_at(deadline.into(), client.search(&request)).await
        };
        match result {
            Ok(Ok(response)) => {
                if response.assets.items.is_empty() {
                    break;
                }

                let page_result = response
                    .assets
                    .items
                    .into_iter()
                    .map(|item| ScanResult { selected: Some(item.original_path.into()), count: 1 })
                    .reduce(reduce_scan_result)
                    .unwrap();

                search_result = reduce_scan_result(search_result, page_result);

                request.page += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(Err(error)) => {
                eprintln!("Error searching root ({root}): {error:?}");
                if search_result.selected.is_none() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                } else {
                    break;
                }
            }
            // Timeout
            Err(_) => (),
        }
    }

    Ok(search_result)
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest<'a> {
    #[serde(borrow, rename = "originalPath")]
    pub original_path: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchPageRequest<'a> {
    #[serde(borrow, flatten)]
    pub base: SearchRequest<'a>,
    pub page: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchResponse {
    pub assets: SearchAssetsResponse,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchAssetsResponse {
    pub items: Vec<AssetResponse>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetResponse {
    pub original_path: String,
}

#[derive(Debug, Clone)]
pub struct ImmichClient {
    client: reqwest::Client,
    base_url: String,
}

impl ImmichClient {
    pub fn new(url: String, api_key: &str) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        headers.insert("x-api-key", reqwest::header::HeaderValue::from_str(api_key).unwrap());
        let client = reqwest::Client::builder().default_headers(headers).build().unwrap();

        Self { client, base_url: url }
    }

    pub async fn search(
        &self,
        request: &SearchPageRequest<'_>,
    ) -> Result<SearchResponse, reqwest::Error> {
        let response = self
            .client
            .post(format!("{}/api/search/metadata", self.base_url))
            .json(request)
            .send()
            .await?
            .error_for_status()?;
        let response: SearchResponse = response.json().await?;
        Ok(response)
    }
}
