use chrono::{DateTime, Utc};

/// Set conditional headers on a request
/// 
/// see [super::S3RequestBuilder::set_conditional_headers]
pub struct ConditionalHeaders {
    if_match: Option<String>,
    if_modified_since: Option<DateTime<Utc>>,
    if_none_match: Option<String>,
    if_unmodified_since: Option<DateTime<Utc>>,
}

impl Default for ConditionalHeaders {
    fn default() -> Self {
        Self { 
            if_match: None, 
            if_modified_since: None, 
            if_none_match: None, 
            if_unmodified_since: None
        }
    }
}

impl ConditionalHeaders {
    pub fn if_match(&mut self, value: &str) -> &mut Self {
        self.if_match = Some(value.to_owned());
        self
    }

    pub fn if_modified_since(&mut self, datetime: DateTime<Utc>) -> &mut Self {
        self.if_modified_since = Some(datetime);
        self
    }

    pub fn if_none_match(&mut self, value: &str) -> &mut Self {
        self.if_none_match = Some(value.to_owned());
        self
    }

    pub fn if_unmodified_since(&mut self, datetime: DateTime<Utc>) -> &mut Self {
        self.if_unmodified_since = Some(datetime);
        self
    }

    pub(crate) fn get_headers(&self) -> Vec<(String, String)> {
        let mut headers = Vec::new();
        if let Some(value) = &self.if_match {
            headers.push((
                "If-Match".to_string(),
                value.to_owned()
            ));
        }
        if let Some(datetime) = &self.if_modified_since {
            headers.push((
                "If-Modified-Since".to_string(),
                datetime.format("%A, %d %b %Y %H:%M:%S GMT").to_string()
            ));
        }
        if let Some(value) = &self.if_none_match {
            headers.push((
                "If-None-Match".to_string(),
                value.to_owned()
            ));
        }
        if let Some(datetime) = &self.if_unmodified_since {
            headers.push((
                "If-Unmodified-Since".to_string(),
                datetime.format("%A, %d %b %Y %H:%M:%S GMT").to_string()
            ));
        }


        headers
    }
}