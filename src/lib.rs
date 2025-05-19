use std::env;

use anyhow::Result;
use api::{
    get_object::{GetObjectRequest, GetObjectResponse},
    head_object::{HeadObjectRequest, HeadObjectResponse},
    list_buckets::{ListBucketsRequest, ListBucketsResponse},
    list_objects_v2::{ListObjectsV2Request, ListObjectsV2Response},
    put_object::{PutObjectRequest, PutObjectResponse},
    S3Request, S3RequestBuilder, S3RequestData, S3Response, S3ResponseData,
};
use wstd::http::Client;

pub mod api;

const AWS_SERVICE: &str = "s3";

/// The S3Client
pub struct S3Client {
    client: Client,
    access_key: String,
    secret_key: String,
    region: String,

    endpoint: String,
}

impl S3Client {
    /// Create a new s3 client
    ///
    /// Uses region and bucket to create a s3 endpoint in the format
    /// {bucket}.s3.{region}.amazonaws.com
    ///
    /// see [`S3Client::set_endpoint`] to override the endpoint created from
    /// the region and bucket or [`S3Client::new_client`] to set the endpoint.
    pub fn new(access_key: String, secret_key: String, region: String, bucket: String) -> Self {
        let endpoint = format!("{}.{}.{}.amazonaws.com", bucket, AWS_SERVICE, region);

        Self {
            client: Client::new(),
            access_key,
            secret_key,
            region,
            endpoint,
        }
    }

    /// Create a new s3 client
    ///
    /// Endpoint is expected to be an s3 compatible bucket endpoint.
    /// for aws the format is {bucket}.s3.{region}.amazonaws.com, see [`S3Client::new`]
    /// for a nicer setup for aws.r
    pub fn new_client(
        access_key: String,
        secret_key: String,
        region: String,
        endpoint: String,
    ) -> Self {
        Self {
            client: Client::new(),
            access_key,
            secret_key,
            region,
            endpoint,
        }
    }

    /// Create a new s3 client from envs
    ///
    /// Use [S3Client::new_secrets_from_endpoint] to set bucket and region in code.
    ///
    /// <div class="warning">
    /// Panics if AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION and AWS_ENDPOINT_URL_S3 isn't set.
    /// </div>
    pub fn new_from_env() -> Self {
        let access_key = env::var("AWS_ACCESS_KEY_ID").expect("ENV \"AWS_ACCESS_KEY\" isn't set");
        let secret_key =
            env::var("AWS_SECRET_ACCESS_KEY").expect("ENV \"AWS_SECRET_ACCESS_KEY\" isn't set");
        let region = env::var("AWS_DEFAULT_REGION").expect("ENV \"AWS_DEFAULT_REGION\" isn't set");
        let endpoint =
            env::var("AWS_ENDPOINT_URL_S3").expect("ENV \"AWS_ENDPOINT_URL_S3\" isn't set");

        Self {
            client: Client::new(),
            access_key,
            secret_key,
            region,
            endpoint,
        }
    }

    /// Create a new s3 client from envs and the endpoint from args
    ///
    /// Uses region and bucket to create a s3 endpoint in the format
    /// {bucket}.s3.{region}.amazonaws.com
    ///
    /// <div class="warning">
    /// Panics if AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY isn't set.
    /// </div>
    pub fn new_secrets_env_with_endpoint(region: String, bucket: String) -> Self {
        let access_key = env::var("AWS_ACCESS_KEY_ID").expect("ENV \"AWS_ACCESS_KEY\" isn't set");
        let secret_key =
            env::var("AWS_SECRET_ACCESS_KEY").expect("ENV \"AWS_SECRET_ACCESS_KEY\" isn't set");
        let endpoint = format!("{}.{}.{}.amazonaws.com", bucket, AWS_SERVICE, region);

        Self {
            client: Client::new(),
            access_key,
            secret_key,
            region,
            endpoint,
        }
    }

    /// Set the bucket endpoint to use
    pub fn set_endpoint(&mut self, endpoint: String) {
        self.endpoint = endpoint;
    }

    /// Get the bucket endpoint in use
    pub fn endpoint(&self) -> &String {
        &self.endpoint
    }

    /// Send a request
    ///
    /// # Examples
    /// ```
    /// let client = S3Client::new_from_env();
    ///
    /// let request = client.new_request_builder(GetObjectRequest {
    ///     key: "myobject".to_string(),
    ///     part_number: None,
    ///     version_id: None,
    /// })?
    /// .query("versionId", "myversionid")
    /// .build()?;
    ///
    /// let resp = client.send(request)?;
    /// ```
    pub async fn send<T>(&self, request: S3Request<T>) -> Result<S3Response<T>>
    where
        T: S3ResponseData,
    {
        let resp = self.client.send(request.request).await?;
        S3Response::from_response(resp)
    }

    /// Create a request builder from a request
    ///
    /// Sets the access_key, secret_key, region and endpoint from the S3Client.
    pub fn new_request_builder<T>(&self, request: T) -> Result<S3RequestBuilder<T::ResponseType>>
    where
        T: S3RequestData,
        <T as S3RequestData>::ResponseType: S3ResponseData,
    {
        request.into_builder(
            &self.access_key,
            &self.secret_key,
            &self.region,
            &self.endpoint,
        )
    }

    /// Send a head_object request
    ///
    /// returns [api::head_object::HeadObjectResponse]
    ///
    /// if you need to set any specific headers or query string values look at
    /// [S3Client::send] and [S3Client::new_request_builder]
    ///
    /// # Examples
    /// ```
    /// use http::StatusCode;
    ///
    /// let client = S3Client::new_from_env();
    ///
    /// let resp = self.head_object(HeadObjectRequest::from_key("myobject"))?;
    /// assert_eq(resp.status(), StatusCode::OK);
    /// ```
    pub async fn head_object(
        &self,
        request: HeadObjectRequest,
    ) -> Result<S3Response<HeadObjectResponse>> {
        let req = request
            .into_builder(
                &self.access_key,
                &self.secret_key,
                &self.region,
                &self.endpoint,
            )?
            .build()?;

        self.send(req).await
    }

    /// Send a get_object request
    /// see [api::get_object::GetObjectRequest]
    ///
    /// returns [api::get_object::GetObjectResponse]
    ///
    /// if you need to set any specific headers or query string values look at
    /// [S3Client::send] and [S3Client::new_request_builder]
    ///
    /// # Examples
    /// ```
    /// use http::StatusCode;
    ///
    /// let client = S3Client::new_from_env();
    ///
    /// let resp = self.get_object(GetObjectRequest::from_key("myobject"))?;
    /// let (head, mut body) = resp.into_parts();
    /// if head.status() == StatusCode::OK {
    ///     let data = body.bytes();
    /// }
    /// ```
    pub async fn get_object(
        &self,
        request: GetObjectRequest,
    ) -> Result<S3Response<GetObjectResponse>> {
        let req = request
            .into_builder(
                &self.access_key,
                &self.secret_key,
                &self.region,
                &self.endpoint,
            )?
            .build()?;

        self.send(req).await
    }

    /// Send a list_buckets request
    /// see [api::list_buckets::ListBucketsRequest]
    ///
    /// returns [api::list_buckets::ListBucketsResponse]
    ///
    /// if you need to set any specific headers or query string values look at
    /// [S3Client::send] and [S3Client::new_request_builder]
    ///
    /// # Examples
    /// ```
    /// use http::StatusCode;
    ///
    /// let client = S3Client::new_from_env();
    ///
    /// let resp = self.list_buckets(ListBucketsRequest::default())?;
    /// if resp.status() == StatusCode::OK {
    ///     let buckets = resp.into_response_data().buckets;
    /// }
    /// ```
    pub async fn list_buckets(
        &self,
        request: ListBucketsRequest,
    ) -> Result<S3Response<ListBucketsResponse>> {
        let req = request
            .into_builder(
                &self.access_key,
                &self.secret_key,
                &self.region,
                &self.endpoint,
            )?
            .build()?;

        self.send(req).await
    }

    /// Send a list_objects_v2 request
    /// see [api::list_objects_v2::ListObjectsV2Request]
    ///
    /// returns [api::list_objects_v2::ListObjectsV2Response]
    ///
    /// if you need to set any specific headers or query string values look at
    /// [S3Client::send] and [S3Client::new_request_builder]
    ///
    /// # Examples
    /// ```
    /// use http::StatusCode;
    ///
    /// let client = S3Client::new_from_env();
    ///
    /// let resp = self.list_objects_v2(ListObjectsV2Request::default())?;
    /// if resp.status() == StatusCode::OK {
    ///     let objects = resp.into_response_data().contents;
    /// }
    /// ```
    pub async fn list_objects_v2(
        &self,
        request: ListObjectsV2Request,
    ) -> Result<S3Response<ListObjectsV2Response>> {
        let req = request
            .into_builder(
                &self.access_key,
                &self.secret_key,
                &self.region,
                &self.endpoint,
            )?
            .build()?;

        self.send(req).await
    }

    /// Send a put_object request
    /// see [api::put_object::PutObjectRequest]
    ///
    /// returns [api::put_object::PutObjectResponse]
    ///
    /// if you need to set any specific headers or query string values look at
    /// [S3Client::send] and [S3Client::new_request_builder]
    ///
    /// # Examples
    /// ```
    /// use http::StatusCode;
    ///
    /// let client = S3Client::new_from_env();
    ///
    /// let mut file = File::open("myfile.txt")?;
    /// let mut contents = Vec::new();
    /// file.read_to_end(&mut contents);
    ///
    /// let resp = self.list_buckets(PutObjectRequest {
    ///     key: "myobject".to_string(),
    ///     body: contents
    /// })?;
    /// assert_eq(resp.status(), StatusCode::OK);
    /// ```
    pub async fn put_object(
        &self,
        request: PutObjectRequest,
    ) -> Result<S3Response<PutObjectResponse>> {
        let req = request
            .into_builder(
                &self.access_key,
                &self.secret_key,
                &self.region,
                &self.endpoint,
            )?
            .build()?;

        self.send(req).await
    }
}
