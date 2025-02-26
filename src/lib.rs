use std::env;

use anyhow::Result;
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use wstd::http::{Client, IntoBody, Method, Request};

fn get_signature_key(secret_key: &str, date: &str, region: &str, service: &str) -> Result<Vec<u8>> {
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes())?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, service.as_bytes())?;
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

pub struct S3Client {
    client: Client,
    access_key: String,
    secret_key: String,
    region: String,
    host: String,
}

impl S3Client {
    pub fn new(access_key: String, secret_key: String, region: String, host: String) -> Self {
        Self {
            client: Client::new(),
            access_key,
            secret_key,
            region,
            host,
        }
    }

    /// Panics if AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION and AWS_ENDPOINT_URL_S3 isn't set.
    pub fn new_from_env() -> Self {
        let access_key = env::var("AWS_ACCESS_KEY_ID").expect("ENV \"AWS_ACCESS_KEY\" isn't set");
        let secret_key =
            env::var("AWS_SECRET_ACCESS_KEY").expect("ENV \"AWS_SECRET_ACCESS_KEY\" isn't set");
        let region = env::var("AWS_DEFAULT_REGION").expect("ENV \"AWS_DEFAULT_REGION\" isn't set");
        let host = env::var("AWS_ENDPOINT_URL_S3").expect("ENV \"AWS_ENDPOINT_URL_S3\" isn't set");

        Self {
            client: Client::new(),
            access_key,
            secret_key,
            region,
            host,
        }
    }

    pub async fn put_object(&self, object_key: &str, payload: &[u8]) -> Result<()> {
        let service = "s3";
        let algorithm = "AWS4-HMAC-SHA256";

        // Get current time in AWS format
        let now = Utc::now();
        let date_stamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

        // Host and endpoint
        let endpoint = format!("https://{}/{object_key}", self.host);

        // SHA-256 hash of the payload
        let payload_hash = hex::encode(Sha256::digest(payload));

        // Canonical Request
        let canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n",
            self.host
        );
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request =
            format!("PUT\n/{object_key}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}",);
        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        // String-to-Sign
        let credential_scope = format!("{date_stamp}/{}/{service}/aws4_request", self.region);
        let string_to_sign =
            format!("{algorithm}\n{amz_date}\n{credential_scope}\n{canonical_request_hash}");

        let signing_key = get_signature_key(&self.secret_key, &date_stamp, &self.region, service)?;

        // Compute the Signature
        let mut mac = Hmac::<Sha256>::new_from_slice(&signing_key)?;
        mac.update(string_to_sign.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        // Authorization Header
        let authorization_header = format!(
            "{algorithm} Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}", self.access_key
        );

        let request = Request::builder()
            .uri(endpoint)
            .method(Method::PUT)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", authorization_header)
            .header("content-length", payload.len().to_string())
            .body(payload.into_body())?;

        let res = self.client.send(request).await?;
        let (parts, mut body) = res.into_parts();

        if parts.status != 200 {
            let bytes = body.bytes().await?;
            let message = std::str::from_utf8(&bytes)?;
            anyhow::bail!(
                "Filed to put object to S3 bucket, HTTP code: {}, Message: {message}",
                parts.status
            )
        }

        Ok(())
    }
}
