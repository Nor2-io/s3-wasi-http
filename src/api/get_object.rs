use anyhow::{anyhow, Result};
use wstd::{
    http::{body::IncomingBody, Method},
    io::AsyncRead,
};

use super::{S3RequestBuilder, S3RequestData, S3ResponseData};

pub struct GetObjectRequest {
    pub key: String,
    pub part_number: Option<i32>,
    pub version_id: Option<String>,
}

impl GetObjectRequest {
    pub fn from_key(key: &str) -> Self {
        GetObjectRequest {
            key: key.to_owned(),
            part_number: None,
            version_id: None,
        }
    }
}

impl S3RequestData for GetObjectRequest {
    type ResponseType = GetObjectResponse;

    fn into_builder(
        &self,
        access_key: &str,
        secret_key: &str,
        region: &str,
        endpoint: &str,
    ) -> Result<S3RequestBuilder<Self::ResponseType>> {
        let mut builder = S3RequestBuilder::new(
            Method::GET,
            &self.key,
            access_key,
            secret_key,
            region,
            endpoint,
        );

        if let Some(part_number) = self.part_number {
            if part_number >= 1 && part_number <= 10000 {
                builder.query("partNumber", Some(&part_number.to_string()));
            } else {
                return Err(anyhow!("part_number has to be constrained to part_number >= 1 and part_number <= 10000, part_number is {part_number}"));
            }
        }
        if let Some(version_id) = &self.version_id {
            builder.query("versionId", Some(version_id));
        }

        Ok(builder)
    }
}

pub struct GetObjectResponse {
    pub data: Vec<u8>,
}

impl S3ResponseData for GetObjectResponse {
    async fn parse_body(response: &mut IncomingBody) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let mut data = Vec::<u8>::new();
        response.read_to_end(&mut data).await?;
        Ok(Self { data })
    }
}
