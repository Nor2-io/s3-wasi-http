use anyhow::Result;
use wstd::http::{body::IncomingBody, Method};

use super::{S3RequestBuilder, S3RequestData, S3ResponseData};

pub struct PutObjectRequest {
    pub key: String,
    pub body: Vec<u8>,
}

impl S3RequestData for PutObjectRequest {
    type ResponseType = PutObjectResponse;

    fn into_builder(
        &self,
        access_key: &str,
        secret_key: &str,
        region: &str,
        endpoint: &str,
    ) -> Result<S3RequestBuilder<Self::ResponseType>> {
        let mut builder = S3RequestBuilder::new(
            Method::PUT,
            &self.key,
            access_key,
            secret_key,
            region,
            endpoint,
        );
        builder.body(&self.body);

        Ok(builder)
    }
}

pub struct PutObjectResponse {}

impl S3ResponseData for PutObjectResponse {
    async fn parse_body(_response: &mut IncomingBody) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {})
    }
}
