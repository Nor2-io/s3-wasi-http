use wstd::http::{body::IncomingBody, Method};
use anyhow::{Result, anyhow};

use super::{S3RequestBuilder, S3RequestData, S3ResponseData};

pub struct HeadObjectRequest {
    pub key: String,
    pub part_number: Option<u32>,
    pub version_id: Option<String>,
}

impl HeadObjectRequest {
    pub fn from_key(key: &str) -> Self {
        Self { 
            key: key.to_owned(), 
            part_number: None, 
            version_id: None
        }
    }
}

impl S3RequestData for HeadObjectRequest {
    type ResponseType = HeadObjectResponse;

    fn into_builder(
        &self, 
        access_key: &str, 
        secret_key: &str, 
        region: &str, 
        endpoint: &str) -> Result<S3RequestBuilder<Self::ResponseType>> {
        let mut builder = S3RequestBuilder::new(
            Method::GET, 
            &self.key, 
            access_key, 
            secret_key, 
            region, 
            endpoint
        );

        if let Some(part_number) = self.part_number {
            if part_number >= 1   && part_number <= 10000 {
                builder.query("partNumber", Some(&part_number.to_string()));
            } else {
                return Err(anyhow!("part_number has to be constrained to 1 <= part_number <= 10000, part_number is {part_number}"))
            }
        }
        if let Some(version_id) = &self.version_id {
            builder.query("VersionId", Some(version_id));
        }

        Ok(builder)
    }
}

pub struct HeadObjectResponse {}

impl S3ResponseData for HeadObjectResponse {
    async fn parse_body(_response: &mut IncomingBody)
         -> anyhow::Result<Self> where Self: Sized {
        Ok(Self {})
    }
}