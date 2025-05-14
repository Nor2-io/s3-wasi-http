use std::i32;

use chrono::{DateTime, Utc};
use wstd::{http::{body::IncomingBody, Method}, io::AsyncRead};
use anyhow::{Result, anyhow};
use xml::{reader::XmlEvent, EventReader};

use super::{
    checksum_algorithm_from_str, x_amz_headers::{
        storage_class_from_str, 
        XAmzStorageClass
    }, 
    ApiChecksumType, 
    ApiObject, 
    ApiOwner, 
    ApiRestoreStatus, 
    S3RequestBuilder, 
    S3RequestData, 
    S3ResponseData
};



pub struct ListObjectsV2Request {
    pub token: Option<String>,
    pub delimiter: Option<char>,
    pub encoding_type: Option<String>,
    pub fetch_owner: bool,
    pub max_keys: Option<i32>,
    pub start_after: Option<String>,
}

impl Default for ListObjectsV2Request {
    fn default() -> Self {
        Self { 
            token: None, 
            delimiter: None, 
            encoding_type: None, 
            fetch_owner: false, 
            max_keys: None, 
            start_after: None 
        }
    }
}

impl S3RequestData for ListObjectsV2Request {
    type ResponseType = ListObjectsV2Response;

    fn into_builder(
        &self, 
        access_key: &str, 
        secret_key: &str, 
        region: &str, 
        endpoint: &str) 
        -> Result<S3RequestBuilder<Self::ResponseType>>{
        let mut builder = S3RequestBuilder::new(
            Method::GET, 
            "/", 
            access_key, 
            secret_key, 
            region, 
            endpoint
        );
        builder.query("list-type", Some("2"));

        if let Some(token) = &self.token {
            builder.query(
                "continuation-token", 
                Some(token)
            );
        }
        if let Some(delimiter) = &self.delimiter {
            builder.query(
                "delimiter", 
                Some(&delimiter.to_string())
            );
        }
        if let Some(encoding_type) = &self.encoding_type {
            builder.query(
                "encoding-type", 
                Some(encoding_type)
            );
        }
        if self.fetch_owner {
            builder.query(
                "fetch-owner", 
                Some("true")
            );
        }
        if let Some(max_keys) = self.max_keys {
            builder.query(
                "max-keys", 
                Some(&max_keys.to_string())
            );
        }
        if let Some(start_after) = &self.start_after {
            builder.query(
                "start-after", 
                Some(start_after)
            );
        }

        Ok(builder)
    }
}


pub struct ListObjectsV2Response {
    pub common_prefixes: Vec<String>,
    pub contents: Vec<ApiObject>,
    pub encoding_type: Option<String>,
    pub is_truncated: bool,
    pub key_count: i32,
    pub max_keys: i32,
    pub name: String,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub start_after: Option<String>,
}

impl S3ResponseData for ListObjectsV2Response {
    async fn parse_body(response: &mut IncomingBody) 
        -> Result<Self> where Self: Sized {
        let mut data = Vec::<u8>::new();
        response.read_to_end(&mut data).await?;
        let mut parser = EventReader::new(data.as_slice());


        let mut list_objects_response = ListObjectsV2Response {
            common_prefixes: Vec::new(),
            contents: Vec::new(),
            encoding_type: None,
            is_truncated: false,
            key_count: 0,
            max_keys: 0,
            name: String::new(),
            continuation_token: None,
            next_continuation_token: None,
            prefix: None,
            delimiter: None,
            start_after: None,
        };
        loop {
            let element = parser.next()?;

            match element {
                xml::reader::XmlEvent::EndDocument => break,

                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "IsTruncated" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        let bool_value = match value.to_lowercase() {
                            v if v == "true" => true,
                            v if v == "false" => false,
                            _ => {
                                return Err(anyhow!("Invalid response object, IsTruncated is not a boolean, value: {value}"))
                            }
                        };

                        list_objects_response.is_truncated = bool_value;
                    } else {
                        return Err(anyhow!("Invalid response object, Name has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "Name" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_objects_response.name = value;
                    } else {
                        return Err(anyhow!("Invalid response object, Name has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "Prefix" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_objects_response.prefix = Some(value);
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "Delimiter" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_objects_response.delimiter = Some(value);
                    } else {
                        return Err(anyhow!("Invalid response object, Delimiter has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "MaxKeys" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        match value.parse::<i32>() {
                            Ok(value) => {
                                list_objects_response.max_keys = value;
                            },
                            Err(e) => {
                                return Err(e.into())
                            },
                        }
                    } else {
                        return Err(anyhow!("Invalid response object, Name has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "EncodingType" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_objects_response.encoding_type = Some(value);
                    } else {
                        return Err(anyhow!("Invalid response object, Name has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "KeyCount" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        match value.parse::<i32>() {
                            Ok(value) => {
                                list_objects_response.key_count = value;
                            },
                            Err(e) => {
                                return Err(e.into())
                            },
                        }
                    } else {
                        return Err(anyhow!("Invalid response object, Name has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "ContinuationToken" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_objects_response.continuation_token = Some(value);
                    } else {
                        return Err(anyhow!("Invalid response object, Name has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "NextContinuationToken" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_objects_response.next_continuation_token = Some(value);
                    } else {
                        return Err(anyhow!("Invalid response object, Name has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "StartAfter" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_objects_response.start_after = Some(value);
                    } else {
                        return Err(anyhow!("Invalid response object, Name has no value"))
                    }
                },
                
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "CommonPrefixes" => {
                    if let XmlEvent::StartElement { name, .. } = parser.next()? {
                        if name.local_name == "Prefix" {
                            if let XmlEvent::Characters(value) = parser.next()? {
                                list_objects_response.common_prefixes.push(value);
                            } else {
                                return Err(anyhow!("Invalid response object, CommonPrefixes.Prefix has no value"))
                            }
                        } else {
                            return Err(anyhow!("Invalid response object, CommonPrefixes has no value"))
                        }
                    } else {
                        return Err(anyhow!("Invalid response object, CommonPrefixes has no value"))
                    }
                },
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "Contents" => {
                    let mut api_object = ApiObject {
                        checksum_algorithm: None,
                        checksum_type: None,
                        etag: String::new(),
                        key: String::new(),
                        last_modified: Utc::now(),
                        owner: None,
                        restore_status: None,
                        size: 0,
                        storage_class: XAmzStorageClass::Standard,
                    };
                    loop {
                        match parser.next()? {
                            XmlEvent::EndElement { name } if name.local_name == "Contents" => break,

                            XmlEvent::StartElement { name, .. } if name.local_name == "ChecksumAlgorithm" => {
                                if let XmlEvent::Characters(value) = parser.next()? {
                                    api_object.checksum_algorithm = Some(checksum_algorithm_from_str(value));
                                } else {
                                    return Err(anyhow!("Invalid response object, ChecksumAlgorithm has no value"))
                                }
                            },
                            XmlEvent::StartElement { name, .. } if name.local_name == "ChecksumType" => {
                                if let XmlEvent::Characters(value) = parser.next()? {
                                    let checksum_type = match value {
                                        v if v == "" => ApiChecksumType::Composite,
                                        v if v == "" => ApiChecksumType::FullObject,

                                        _ => {
                                            return Err(anyhow!("Invalid response object, ChecksumType has an invalid type, type: {value}"))
                                        }
                                    };

                                    api_object.checksum_type = Some(checksum_type);
                                } else {
                                    return Err(anyhow!("Invalid response object, ChecksumType has no value"))
                                }
                            },
                            XmlEvent::StartElement { name, .. } if name.local_name == "ETag" => {
                                if let XmlEvent::Characters(value) = parser.next()? {
                                    api_object.etag = value;
                                } else {
                                    return Err(anyhow!("Invalid response object, Etag has no value"))
                                }
                            },
                            XmlEvent::StartElement { name, .. } if name.local_name == "Key" => {
                                if let XmlEvent::Characters(value) = parser.next()? {
                                    api_object.key = value
                                } else {
                                    return Err(anyhow!("Invalid response object, Key has no value"))
                                }
                            },
                            XmlEvent::StartElement { name, .. } if name.local_name == "LastModified" => {
                                if let XmlEvent::Characters(value) = &parser.next()? {
                                    let datetime =  DateTime::parse_from_rfc3339(&value)?.to_utc();
                                    api_object.last_modified = datetime;
                                } else {
                                    return Err(anyhow!("Invalid response object, LastModified has no value"))
                                }
                            },
                            XmlEvent::StartElement { name, .. } if name.local_name == "Size" => {
                                if let XmlEvent::Characters(value) = parser.next()? {
                                    match value.parse::<usize>() {
                                        Ok(value) => {
                                            api_object.size = value;
                                        },
                                        Err(e) => {
                                            return Err(e.into())
                                        },
                                    }
                                } else {
                                    return Err(anyhow!("Invalid response object, Name has no value"))
                                }
                            },
                            XmlEvent::StartElement { name, .. } if name.local_name == "StorageClass" => {
                                if let XmlEvent::Characters(value) = parser.next()? {
                                    api_object.storage_class = storage_class_from_str(value);
                                } else {
                                    return Err(anyhow!("Invalid response object, StorageClass has no value"))
                                }
                            },

                            XmlEvent::StartElement { name, .. } if name.local_name == "Owner" => {
                                let mut owner = ApiOwner {
                                    display_name: None,
                                    id: String::new(),
                                };
                                loop {
                                    match parser.next()? {
                                        XmlEvent::StartElement { name, .. } => {
                                            if let XmlEvent::Characters(value) = parser.next()? {
                                                if name.local_name == "DisplayName" {
                                                    owner.display_name = Some(value);
                                                } else if name.local_name == "ID" {
                                                    owner.id = value;
                                                }
                                            } else {
                                                return Err(anyhow!("Invalid response object, {name} element has no value"))
                                            }
                                        },
                                        XmlEvent::EndElement { name } if name.local_name == "Owner" => break, 
                                        _ => {}
                                    }
                                }

                                api_object.owner = Some(owner);
                            },
                            XmlEvent::StartElement { name, .. } if name.local_name == "RestoreStatus" => {
                                let mut restore_status = ApiRestoreStatus {
                                    is_restore_in_progress: false,
                                    restore_expiry_date: Utc::now(),
                                };

                                loop {
                                    match parser.next()? {
                                        XmlEvent::StartElement { name, .. } => {
                                            if let XmlEvent::Characters(value) = parser.next()? {
                                                if name.local_name == "IsRestoreInProgress" {
                                                    let bool_value = match value.to_lowercase() {
                                                        v if v == "true" => true,
                                                        v if v == "false" => false,
                                                        _ => {
                                                            return Err(anyhow!("Invalid response object, RestoreStatus.IsRestoreInProgress is not a boolean, value: {value}"))
                                                        }
                                                    };

                                                    restore_status.is_restore_in_progress = bool_value;
                                                } else if name.local_name == "RestoreExpiryDate" {
                                                    let datetime =  DateTime::parse_from_rfc3339(&value)?.to_utc();
                                                    restore_status.restore_expiry_date = datetime;
                                                }
                                            } else {
                                                return Err(anyhow!("Invalid response object, {name} element has no value"))
                                            }
                                        },
                                        XmlEvent::EndElement { name } if name.local_name == "Owner" => break, 
                                        _ => {}
                                    }
                                }

                                api_object.restore_status = Some(restore_status)
                            },
                            
                            _ => {}
                        }
                    }

                    list_objects_response.contents.push(api_object);
                },

                _ => {}
            }
        }

        Ok(list_objects_response)
    }
}