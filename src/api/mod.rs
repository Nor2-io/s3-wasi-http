use std::{marker::PhantomData, str::FromStr};

use conditional_headers::ConditionalHeaders;
use content_headers::ContentHeaders;
use x_amz_headers::{XAmzHeaders, XAmzStorageClass};

use http::{response::Parts, StatusCode};
use percent_encoding::{AsciiSet, CONTROLS};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use wstd::http::{body::{BoundedBody, IncomingBody}, HeaderName, HeaderValue, IntoBody, Method, Request, Response, Scheme, Uri};

use crate::AWS_SERVICE;

pub mod get_object;
pub mod head_object;
pub mod list_buckets;
pub mod list_objects_v2;
pub mod put_object;

pub mod content_headers;
pub mod conditional_headers;
pub mod x_amz_headers;

const AWS_SERVICE_EMPTY_PAYLOAD: &[u8] = "UNSIGNED-PAYLOAD".as_bytes();
const AWS_SIGN_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const QUERY_SET: &AsciiSet = &CONTROLS    
    .add(b' ')
    .add(b'/')
    .add(b':') // Required to be percent encoded to function with aws services
    .add(b',') // Required to be percent encoded to function with aws services
    .add(b'?')
    .add(b'#')
    .add(b'[')
    .add(b']')
    .add(b'{')
    .add(b'}')
    .add(b'|')
    .add(b'@')
    .add(b'!')
    .add(b'$')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b';')
    .add(b'=')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'"')
    .add(b'^')
    .add(b'`')
    .add(b'\\');
const PATH_SET: &AsciiSet = &QUERY_SET.remove(b'/');

pub enum ChecksumAlgorithm {
    CRC32,
    CRC32C,
    SHA1,
    SHA256,
    CRC64NVME,
    Alogrithm(String),
}
pub(crate) fn checksum_algorithm_from_str(algo: String) -> ChecksumAlgorithm {
    match algo.to_lowercase() {
        a if a == "crc32" => ChecksumAlgorithm::CRC32,
        a if a == "crc32c" => ChecksumAlgorithm::CRC32C,
        a if a == "sha1" => ChecksumAlgorithm::SHA1,
        a if a == "sha256" => ChecksumAlgorithm::SHA256,
        a if a == "crc64nvme" => ChecksumAlgorithm::CRC64NVME,
        
        a => ChecksumAlgorithm::Alogrithm(a)
    }
}

pub enum ApiChecksumType {
    Composite,
    FullObject,
}

pub struct ApiRestoreStatus {
    pub is_restore_in_progress: bool,
    pub restore_expiry_date: DateTime<Utc>,
}

pub struct ApiObject {
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ApiChecksumType>,
    pub etag: String,
    pub key: String,
    pub last_modified: DateTime<Utc>,
    pub owner: Option<ApiOwner>,
    pub restore_status: Option<ApiRestoreStatus>,
    pub size: usize,
    pub storage_class: XAmzStorageClass,
}

pub struct ApiBucket {
    pub name: String,
    pub creation_date: Option<DateTime<Utc>>,
    pub region: String,
}

pub struct ApiOwner {
    pub display_name: Option<String>,
    pub id: String,
}


pub trait S3RequestData {
    type ResponseType;
    /// Creates an S3RequestBuilder from the S3RequestData object
    fn into_builder(
        &self, 
        access_key: &str, 
        secret_key: &str, 
        region: &str, 
        endpoint: &str) 
        -> Result<S3RequestBuilder<Self::ResponseType>> 
            where <Self as S3RequestData>::ResponseType: S3ResponseData;
}

pub struct S3Request<T>
    where T: S3ResponseData {
    pub request: Request<BoundedBody<Vec<u8>>>,
    phantom: PhantomData<T>,
}

pub trait S3ResponseData {
    /// Parse the response body into a S3ResponseData struct
    #[allow(async_fn_in_trait)] 
    async fn parse_body(
        response: &mut IncomingBody) 
        -> Result<Self> where Self: Sized;
}


pub struct S3Response<T>
    where T: S3ResponseData {
    head: Parts,
    body: IncomingBody,
    phantom: PhantomData<T>,
}

impl<T> S3Response<T> 
    where T: S3ResponseData {
    pub fn from_response(response: Response<IncomingBody>) -> Result<Self> {
        let (head, body) = response.into_parts();
        Ok(Self {
            head,
            body,
            phantom: PhantomData,
        })
    }

    pub fn status(&self) -> StatusCode {
        self.head.status
    }

    pub fn into_parts(self) -> (Parts, IncomingBody) {
        (self.head, self.body)
    }

    /// Parse response body into an S3ResponseData struct
    pub async fn into_response_data(&mut self) -> Result<T> {
        T::parse_body(&mut self.body).await
    }

    /// Parse response body into an S3ResponseData struct and get headers
    pub async fn into_response_data_parts(&mut self) -> Result<(Parts, T)> {
        let body = T::parse_body(&mut self.body).await?;
        Ok((self.head.clone(), body))
    }
}



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

fn percent_encode_query<T: AsRef<str>>(value: T) -> String {
    percent_encoding::utf8_percent_encode(value.as_ref(), QUERY_SET).to_string()
}
fn percent_encode_path<T: AsRef<str>>(value: T) -> String {
    percent_encoding::utf8_percent_encode(value.as_ref(), PATH_SET).to_string()
}

/// Build and sign an s3 request
pub struct S3RequestBuilder<T: S3ResponseData> {
    pub(crate) method: Method,
    pub(crate) action: String,
    pub(crate) query: Vec<(String, String)>,
    pub(crate) headers: Vec<(String, String)>,

    pub(crate) x_amz_headers: Vec<(String, String)>,

    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    pub(crate) region: String,
    pub(crate) endpoint: String,

    pub(crate) scheme: Scheme,

    pub(crate) body: Option<Vec<u8>>,

    phantom: PhantomData<T>,
}

impl<T> S3RequestBuilder<T>
    where T: S3ResponseData {
    /// Create a new S3RequestBuilder
    /// 
    /// See [crate::S3Client::new_request_builder]
    pub(crate) fn new(
        method: Method,
        action: &str,
        access_key: &str, 
        secret_key: &str, 
        region: &str, 
        endpoint: &str) -> Self {
        Self { 
            method, 
            action: action.to_owned(), 
            query: Vec::new(), 
            headers: Vec::new(),
            x_amz_headers: Vec::new(),
            access_key: access_key.to_owned(), 
            secret_key: secret_key.to_owned(), 
            region: region.to_owned(), 
            endpoint: endpoint.to_owned(),
            scheme: Scheme::HTTPS,
            body: None,
            phantom: PhantomData,
        }
    }

    pub(crate) fn method(&mut self, method: Method) -> &mut Self {
        self.method = method;
        self
    }
    pub(crate) fn action(&mut self, action: &str) -> &mut Self {
        self.action = percent_encode_path(action);
        self
    }

    /// Add a query string
    pub fn query(&mut self, key: &str, value: Option<&str>) -> &mut Self {
        let str_value = match value {
            Some(v) => percent_encode_query(v),
            None => percent_encode_query(""),
        };
        self.query.push((percent_encode_query(key), str_value));
        self
    }
    /// Add a header
    pub fn header(&mut self, key: &str, value: &str) -> &mut Self {
        if key.starts_with("x-amz") {
            self.x_amz_headers.push((key.to_owned(), value.to_owned()));
            self
        } else {
            self.headers.push((key.to_owned(), value.to_owned()));
            self
        }
    }
    /// Set the request body
    pub fn body<B>(&mut self, body: B) -> &mut Self 
        where B: AsRef<[u8]> {
        let b = body.as_ref().to_vec();
        self.body = Some(b);
        self
    }
    /// Set request scheme
    pub fn scheme(&mut self, scheme: Scheme) -> &mut Self {
        self.scheme = scheme;
        self
    }

    /// Set the request content headers
    /// 
    /// see [ContentHeaders]
    /// [S3RequestBuilder::headers] can be easier if adding a small amount of headers
    pub fn set_content_headers(&mut self, content: &ContentHeaders) -> &mut Self {
        let mut content_headers = content.get_headers();
        self.headers.append(&mut content_headers);
        self
    }
    /// Set the request content query string will 
    /// also set the range header if set
    /// 
    /// see [ContentHeaders]
    /// [S3RequestBuilder::query] can be easier if adding a small amount of queries
    pub fn set_content_query(&mut self, content: &ContentHeaders) -> &mut Self {
        let query = content.get_query();
        for (key, value) in query {
            self.query(&key, Some(&value));
        }
        self
    }
    /// Set the request conditional headers
    /// 
    /// see [ConditionalHeaders]
    /// [S3RequestBuilder::headers] can be easier if adding a small amount of headers
    pub fn set_conditional_headers(&mut self, conds: &ConditionalHeaders) -> &mut Self {
        let mut conditional_headers = conds.get_headers();
        self.headers.append(&mut conditional_headers);
        self
    }
    /// Set the request x-amz headers
    /// 
    /// See [XAmzHeaders]
    /// [S3RequestBuilder::headers] can be easier if adding a small amount of headers
    pub fn set_x_amz_headers(&mut self, xamz: &XAmzHeaders)  -> &mut Self {
        let mut xamz_headers = xamz.get_headers();
        self.x_amz_headers.append(&mut xamz_headers);
        self
    }

    /// Set authentication values
    pub fn set_auth(&mut self, access_key: &str, secret_key: &str, region: &str, endpoint: &str) -> &mut Self {
        self.access_key = access_key.to_owned();
        self.secret_key = secret_key.to_owned();
        self.region = region.to_owned();
        self.endpoint = endpoint.to_owned();
        self
    }


    /// Build and sign the request
    pub fn build(&mut self) -> Result<S3Request<T>> {
        // Get current time in AWS format
        let now = Utc::now();
        let date_stamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

        // Query string for the canonical request
        let query = match self.query.is_empty() {
            true => "".to_string(),
            false => {
                self.query.sort();
                self.query.iter().map(|(k,v)| {
                    format!("{k}={v}")
                }).collect::<Vec<String>>().join("&")
            },
        };

        // SHA-256 hash of the payload
        let payload_hash = match &self.body {
            Some(b) => hex::encode(Sha256::digest(&b)),
            None => hex::encode(Sha256::digest(AWS_SERVICE_EMPTY_PAYLOAD)),
        };

        // Get host from the uri
        let host_uri = Uri::from_str(&self.endpoint)?;
        let (scheme, host) = match (host_uri.scheme(), host_uri.host()) {
            (None, Some(host)) => (&self.scheme, host),
            (Some(scheme), Some(host)) => (scheme, host),
            (_, None) => {
                return Err(anyhow!("No host defined"));
            },
        };

        // Canonical Request
        let mut canonical_headers_vec = match self.x_amz_headers.is_empty() {
            true => Vec::new(),
            false => {
                self.x_amz_headers.clone()
            },
        };
        canonical_headers_vec.push(("host".to_string(), host.to_string()));
        canonical_headers_vec.push(("x-amz-content-sha256".to_string(), payload_hash.clone()));
        canonical_headers_vec.push(("x-amz-date".to_string(), amz_date.clone()));
        canonical_headers_vec.sort();
        let canonical_headers = canonical_headers_vec.iter().map(|(k,v)| {
            format!("{k}: {v}")
        }).collect::<Vec<String>>().join("\n");
        let signed_headers = canonical_headers_vec.iter().map(|(k, _)| {
            k.to_owned()
        }).collect::<Vec<String>>().join(";");

        let method = self.method.as_str();
        let canonical_request =
        format!("{method}\n/{action}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}", 
                action = self.action);
        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        // String-to-Sign
        let credential_scope = format!("{date_stamp}/{}/{AWS_SERVICE}/aws4_request", self.region);
        let string_to_sign =
            format!("{AWS_SIGN_ALGORITHM}\n{amz_date}\n{credential_scope}\n{canonical_request_hash}");

        let signing_key = get_signature_key(&self.secret_key, &date_stamp, &self.region, AWS_SERVICE)?;

        // Compute the Signature
        let mut mac = Hmac::<Sha256>::new_from_slice(&signing_key)?;
        mac.update(string_to_sign.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        // Authorization Header
        let authorization_header = format!(
            "{AWS_SIGN_ALGORITHM} Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}", self.access_key
        );


        let body = match &self.body {
            Some(b) => &b,
            None => "".as_bytes(),
        };

        let uri = match self.query.is_empty() {
            true => format!("{scheme}://{host}/{}", self.action),
            false => format!("{scheme}://{host}/{}?{query}", self.action),
        };
        let mut builder = Request::builder()
            .uri(uri)
            .method(&self.method)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", authorization_header)
            .header("content-length", body.len().to_string());

        match builder.headers_mut() {
            Some(headers) => {
                for (key, value) in &self.headers {
                    headers.insert(
                        HeaderName::from_str(&key)?, 
                        HeaderValue::from_str(&value)?);
                }
            },
            None => {},
        };

        let request = S3Request::<T> {
            request: builder.body(body.into_body())?,
            phantom: PhantomData,
            
        };

        Ok(request)
    }
}