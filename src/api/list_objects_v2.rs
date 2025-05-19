use std::i32;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use wstd::{
    http::{body::IncomingBody, Method},
    io::AsyncRead,
};
use xml::{reader::XmlEvent, EventReader};

use super::{
    checksum_algorithm_from_str, parse_xml_bool, parse_xml_string, parse_xml_value,
    x_amz_headers::{storage_class_from_str, XAmzStorageClass},
    ApiChecksumType, ApiObject, ApiOwner, ApiRestoreStatus, S3RequestBuilder, S3RequestData,
    S3ResponseData,
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
            start_after: None,
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
        endpoint: &str,
    ) -> Result<S3RequestBuilder<Self::ResponseType>> {
        let mut builder =
            S3RequestBuilder::new(Method::GET, "/", access_key, secret_key, region, endpoint);
        builder.query("list-type", Some("2"));

        if let Some(token) = &self.token {
            builder.query("continuation-token", Some(token));
        }
        if let Some(delimiter) = &self.delimiter {
            builder.query("delimiter", Some(&delimiter.to_string()));
        }
        if let Some(encoding_type) = &self.encoding_type {
            builder.query("encoding-type", Some(encoding_type));
        }
        if self.fetch_owner {
            builder.query("fetch-owner", Some("true"));
        }
        if let Some(max_keys) = self.max_keys {
            builder.query("max-keys", Some(&max_keys.to_string()));
        }
        if let Some(start_after) = &self.start_after {
            builder.query("start-after", Some(start_after));
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
    async fn parse_body(response: &mut IncomingBody) -> Result<Self>
    where
        Self: Sized,
    {
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

                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "IsTruncated" =>
                {
                    list_objects_response.is_truncated =
                        parse_xml_bool(&mut parser, "IsTruncated")?;
                }
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "Name" => {
                    list_objects_response.name = parse_xml_string(&mut parser, "Name")?;
                }
                xml::reader::XmlEvent::StartElement { name, .. } if name.local_name == "Prefix" => {
                    if let XmlEvent::Characters(value) = parser.next()? {
                        list_objects_response.prefix = Some(value);
                    }
                }
                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "Delimiter" =>
                {
                    list_objects_response.delimiter =
                        Some(parse_xml_string(&mut parser, "Delimiter")?);
                }
                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "MaxKeys" =>
                {
                    list_objects_response.max_keys =
                        parse_xml_value::<i32>(&mut parser, "MaxKeys")?;
                }
                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "EncodingType" =>
                {
                    list_objects_response.encoding_type =
                        Some(parse_xml_string(&mut parser, "EncodingType")?);
                }
                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "KeyCount" =>
                {
                    list_objects_response.key_count =
                        parse_xml_value::<i32>(&mut parser, "KeyCount")?;
                }
                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "ContinuationToken" =>
                {
                    list_objects_response.continuation_token =
                        Some(parse_xml_string(&mut parser, "ContinuationToken")?);
                }
                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "NextContinuationToken" =>
                {
                    list_objects_response.next_continuation_token =
                        Some(parse_xml_string(&mut parser, "NextContinuationToken")?);
                }
                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "StartAfter" =>
                {
                    list_objects_response.next_continuation_token =
                        Some(parse_xml_string(&mut parser, "StartAfter")?);
                }

                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "CommonPrefixes" =>
                {
                    if let XmlEvent::StartElement { name, .. } = parser.next()? {
                        if name.local_name == "Prefix" {
                            if let XmlEvent::Characters(value) = parser.next()? {
                                list_objects_response.common_prefixes.push(value);
                            } else {
                                return Err(anyhow!(
                                    "Invalid response object, CommonPrefixes.Prefix has no value"
                                ));
                            }
                        } else {
                            return Err(anyhow!(
                                "Invalid response object, CommonPrefixes has no value"
                            ));
                        }
                    } else {
                        return Err(anyhow!(
                            "Invalid response object, CommonPrefixes has no value"
                        ));
                    }
                }
                xml::reader::XmlEvent::StartElement { name, .. }
                    if name.local_name == "Contents" =>
                {
                    list_objects_response
                        .contents
                        .push(ApiObject::parse(&mut parser)?);
                }

                _ => {}
            }
        }

        Ok(list_objects_response)
    }
}
