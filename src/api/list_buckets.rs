use chrono::DateTime;
use wstd::{http::{body::IncomingBody, Method}, io::AsyncRead};
use anyhow::{Result, anyhow};
use xml::reader::{EventReader, XmlEvent};

use super::{
    parse_xml_string, ApiBucket, ApiOwner, S3RequestBuilder, S3RequestData, S3ResponseData
};

pub struct ListBucketsRequest {
    bucket_region: Option<String>,
    token: Option<String>,
    max_buckets: Option<i32>,
    prefix: Option<String>
}

impl Default for ListBucketsRequest {
    fn default() -> Self {
        Self { 
            bucket_region: None, 
            token: None, 
            max_buckets: None, 
            prefix: None 
        }
    }
}

impl S3RequestData for ListBucketsRequest {
    type ResponseType = ListBucketsResponse;

    fn into_builder(
        &self, 
        access_key: &str, 
        secret_key: &str, 
        region: &str, 
        endpoint: &str) 
        -> Result<S3RequestBuilder<Self::ResponseType>> {
        let mut builder = S3RequestBuilder::new(
            Method::GET, 
            "/", 
            access_key, 
            secret_key, 
            region, 
            endpoint
        );

        if let Some(bucket_region) = &self.bucket_region {
            builder.query(
                "bucket-region", 
                Some(bucket_region)
            );
        }
        if let Some(token) = &self.token {
            builder.query(
                "continuation-token", 
                Some(token)
            );
        }
        if let Some(max_buckets) = self.max_buckets {
            if max_buckets >= 1 && max_buckets <= 10000 {
                builder.query(
                    "max-buckets", 
                    Some(&max_buckets.to_string())
                );
            } else {
                return Err(anyhow!("max_buckets has to be constrained to part_number >= and part_number <= 10000, part_number is {max_buckets}"))
            }
        }
        if let Some(prefix) = &self.prefix {
            builder.query(
                "prefix", 
                Some(prefix)
            );
        }

        Ok(builder)
    }
}


pub struct ListBucketsResponse {
    pub continuation_token: Option<String>,

    pub buckets: Vec<ApiBucket>,
    pub owner: ApiOwner,
    pub prefix: Option<String>,
}

impl S3ResponseData for ListBucketsResponse {
    async fn parse_body(response: &mut IncomingBody) 
        -> Result<Self> where Self: Sized {
        let mut data = Vec::<u8>::new();
        response.read_to_end(&mut data).await?;
        let mut parser = EventReader::new(data.as_slice());
        
        let mut list_bucket_response = ListBucketsResponse {
            continuation_token: None,
            buckets: Vec::new(),
            owner: ApiOwner { 
                display_name: None, 
                id: String::new()
            },
            prefix: None,
        };
        loop {
            match parser.next()? {
                XmlEvent::EndDocument => break,
                
                XmlEvent::StartElement { name, .. } if name.local_name == "Prefix" => {
                    list_bucket_response.prefix = Some(parse_xml_string(&mut parser, "Prefix")?);
                },
                XmlEvent::StartElement { name, .. } if name.local_name == "ContinuationToken" => {
                    list_bucket_response.continuation_token = Some(parse_xml_string(&mut parser, "ContinuationToken")?);
                },
                XmlEvent::StartElement { name, .. } if name.local_name == "Owner" => {
                    list_bucket_response.owner = ApiOwner::parse(&mut parser)?;
                },
                XmlEvent::StartElement { name, .. } if name.local_name == "Buckets" => {
                    loop {
                        match parser.next()? {
                            XmlEvent::EndElement { name } if name.local_name == "Buckets" => break,

                            XmlEvent::StartElement { .. } => { // Bucket
                                list_bucket_response.buckets.push(ApiBucket::parse(&mut parser)?);
                            },

                            _ => {},
                        }
                    }
                },

                _ => {}
            }
        }

        Ok(list_bucket_response)
    }
}