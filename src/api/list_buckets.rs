use chrono::DateTime;
use wstd::{http::{body::IncomingBody, Method}, io::AsyncRead};
use anyhow::{Result, anyhow};
use xml::reader::{EventReader, XmlEvent};

use super::{
    ApiBucket, 
    ApiOwner, 
    S3RequestBuilder, 
    S3RequestData, 
    S3ResponseData
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
                return Err(anyhow!("max_buckets has to be constrained to 1 <= part_number <= 10000, part_number is {max_buckets}"))
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
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_bucket_response.prefix = Some(value);
                    } else {
                        return Err(anyhow!("Invalid response object, Prefix has no value"))
                    }
                },
                XmlEvent::StartElement { name, .. } if name.local_name == "ContinuationToken" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_bucket_response.continuation_token = Some(value);
                    } else {
                        return Err(anyhow!("Invalid response object, ContinuationToken has no value"))
                    }
                },
                XmlEvent::StartElement { name, .. } if name.local_name == "Owner" => {
                    loop {
                        match parser.next()? {
                            XmlEvent::StartElement { name, .. } => {
                                if let XmlEvent::Characters(value) = parser.next()? {
                                    if name.local_name == "DisplayName" {
                                        list_bucket_response.owner.display_name = Some(value);
                                    } else if name.local_name == "ID" {
                                        list_bucket_response.owner.id = value;
                                    }
                                } else {
                                    return Err(anyhow!("Invalid response object, {name} element has no value"))
                                }
                            },
                            XmlEvent::EndElement { name } if name.local_name == "Owner" => break, 
                            _ => {}
                        }
                    }
                },
                XmlEvent::StartElement { name, .. } if name.local_name == "Buckets" => {
                    loop {
                        match parser.next()? {
                            XmlEvent::EndElement { name } if name.local_name == "Buckets" => break,

                            XmlEvent::StartElement { .. } => { // Bucket
                                let mut bucket = ApiBucket {
                                    name: String::new(),
                                    creation_date: None,
                                    region: String::new(),
                                };
                                loop {
                                    match parser.next()? {
                                        XmlEvent::StartElement { name, .. } if name.local_name == "BucketRegion" => {
                                            if let XmlEvent::Characters(value) = parser.next()? {
                                                bucket.region = value;
                                            } else {
                                                return Err(anyhow!("Invalid response object, BucketRegion has no value"))
                                            }
                                        },
                                        XmlEvent::StartElement { name, .. } if name.local_name == "CreationDate" => {
                                            if let XmlEvent::Characters(value) = &parser.next()? {
                                                let datetime =  DateTime::parse_from_rfc3339(&value)?.to_utc();
                                                bucket.creation_date = Some(datetime);
                                            } else {
                                                return Err(anyhow!("Invalid response object, CreationDate has no value"))
                                            }
                                        },
                                        XmlEvent::StartElement { name, .. } if name.local_name == "Name" => {
                                            if let XmlEvent::Characters(value) = parser.next()? {
                                                bucket.name = value;
                                            } else {
                                                return Err(anyhow!("Invalid response object, Name has no value"))
                                            }
                                        },
                                        XmlEvent::EndElement { name } if name.local_name == "Bucket" => {
                                            list_bucket_response.buckets.push(bucket);
                                            break
                                        },
                                        _ => {},
                                    }
                                }
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