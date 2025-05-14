use chrono::{DateTime, Utc};


pub enum CacheControl {
    MaxAge(i32),
    SMaxAge(i32),
    MustRevalidate,
    ProxyRevalidate,
    Private,
    Public,
    MustUnderstand,
    NoTransform,
    Immutable,
    StaleWhileRevalidate(i32),
    StaleIfError(i32),
    NoCache,
    NoStore,
    MaxStale(i32),
    MinFresh(i32),
    OnlyIfCached,
    Value(String),
}

pub enum ContentDisposition {
    Inline,
    Attachment,
    AttachmentWithFile(String),
    Value(String),
}

pub enum ContentEncoding {
    GZIP,
    Compress,
    Deflate,
    Br,
    ZSTD,
    DCB,
    DCZ,
    Encoding(String),
}

/// Set content headers on a request
/// 
/// see [super::S3RequestBuilder::set_content_headers]
pub struct ContentHeaders {
    cache_control: Vec<CacheControl>,
    content_disposition: Option<ContentDisposition>,
    content_encoding: Option<ContentEncoding>,
    content_language: Option<String>,
    content_type: Option<String>,
    expires: Option<DateTime<Utc>>,
    range: Option<(i32, Option<i32>)>,
    content_md5: Option<String>,
}

impl Default for ContentHeaders {
    fn default() -> Self {
        Self { 
            cache_control: Vec::new(), 
            content_disposition: None, 
            content_encoding: None, 
            content_language: None, 
            content_type: None, 
            expires: None, 
            range: None,
            content_md5: None,
        }
    }
}

impl ContentHeaders {
    pub fn add_cache_control(&mut self, cache_control: CacheControl) -> &mut Self {
        self.cache_control.push(cache_control);
        self
    }
    pub fn cache_control(&mut self, cache_control: &mut Vec<CacheControl>) -> &mut Self {
        self.cache_control.append(cache_control);
        self
    }

    pub fn content_disposition(&mut self, disposition: ContentDisposition) -> &mut Self {
        self.content_disposition = Some(disposition);
        self
    }

    pub fn content_encoding(&mut self, encoding: ContentEncoding) -> &mut Self {
        self.content_encoding = Some(encoding);
        self
    }

    pub fn content_language(&mut self, lang: String) -> &mut Self {
        self.content_language = Some(lang);
        self
    }

    pub fn content_type(&mut self, content_type: String) -> &mut Self {
        self.content_type = Some(content_type);
        self
    }

    pub fn expires(&mut self, datetime: DateTime<Utc>) -> &mut Self {
        self.expires = Some(datetime);
        self
    }

    pub fn range(&mut self, start: i32, end: Option<i32>) -> &mut Self {
        self.range = Some((start, end));
        self
    }

    pub fn md5(&mut self, checksum: String) -> &mut Self {
        self.content_md5 = Some(checksum);
        self
    }

    fn get_cache_control_str(&self) -> Option<String> {
        match &self.cache_control.is_empty() {
            true => None,
            false => {
                let mut cc_directives_str = String::new();
                let mut prefix = "";
                for cc in &self.cache_control {
                    let cc_str = match cc {
                        CacheControl::MaxAge(age) => &format!("max-age={age}"),
                        CacheControl::SMaxAge(age) => &format!("s-maxage={age}"),
                        CacheControl::MustRevalidate => "must-revalidate",
                        CacheControl::ProxyRevalidate => "proxy-revalidate",
                        CacheControl::Private => "private",
                        CacheControl::Public => "public",
                        CacheControl::MustUnderstand => "must-understand",
                        CacheControl::NoTransform => "no-transform",
                        CacheControl::Immutable => "immutable",
                        CacheControl::StaleWhileRevalidate(age) => &format!("stale-while-revalidate={age}"),
                        CacheControl::StaleIfError(age) => &format!("stale-if-error={age}"),
                        CacheControl::NoCache => "no-cache",
                        CacheControl::NoStore => "no-store",
                        CacheControl::MaxStale(age) => &format!("max-stale={age}"),
                        CacheControl::MinFresh(age) => &format!("min-fresh={age}"),
                        CacheControl::OnlyIfCached => "only-if-cached",
                        CacheControl::Value(str) => str
                    };

                    cc_directives_str.push_str(prefix);
                    cc_directives_str.push_str(cc_str);

                    prefix = ", ";
                }



                Some(String::from(cc_directives_str))
            }
        }
    }
    
    fn get_content_disposition_str(&self) -> Option<String> {
        match &self.content_disposition {
            Some(d) => {
                let disp = match d {
                    ContentDisposition::Inline => "inline",
                    ContentDisposition::Attachment => "attachment",
                    ContentDisposition::AttachmentWithFile(filename) => {
                        &format!("attachment; filename={filename}")
                    },
                    ContentDisposition::Value(str) => str,
                };

                Some(String::from(disp))
            },
            None => None,
        }
    }

    fn get_content_encoding_str(&self) -> Option<String> {
        match &self.content_encoding {
            Some(encoding) => {
                let e = match encoding {
                    ContentEncoding::GZIP => "gzip",
                    ContentEncoding::Compress => "compress",
                    ContentEncoding::Deflate => "deflate",
                    ContentEncoding::Br => "br",
                    ContentEncoding::ZSTD => "zstd",
                    ContentEncoding::DCB => "dcb",
                    ContentEncoding::DCZ => "dcz",
                    ContentEncoding::Encoding(enc) => enc,
                };

                Some(String::from(e))
            },
            None => None,
        }
    }

    pub(crate) fn get_range_header(&self) -> Option<(String, String)> {
        match &self.range {
            Some((start, end)) => {
                let end_str = match end {
                    Some(e) => e.to_string(),
                    None => String::new(),
                };

                Some((String::from("Range"), format!("bytes={start}-{end_str}")))
            },
            None => None,
        }
    } 

    pub(crate) fn get_headers(&self) -> Vec<(String, String)> {
        let mut headers = Vec::new();
        if let Some(control) = &self.get_cache_control_str() {
            headers.push((
                String::from("Cache-Control"), 
                control.to_owned()
            ));
        }

        if let Some(disposition) = &self.get_content_disposition_str() {
            headers.push((
                String::from("Content-Disposition"), 
                disposition.to_owned()));
        }

        if let Some(encoding) = &self.get_content_encoding_str() {
            headers.push((
                String::from("Content-Encoding"), 
                encoding.to_owned()
            ));
        }

        if let Some(language) = &self.content_language {
            headers.push((
                String::from("Content-Language"), 
                language.to_owned()
            ));
        }

        if let Some(content_type) = &self.content_type {
            headers.push((
                String::from("Content-Type"), 
                content_type.to_owned()
            ));
        }

        if let Some(expires) = &self.expires {
            headers.push((
                String::from("Expires"), 
                expires.format("%A, %d %b %Y %H:%M:%S GMT").to_string()
            ));
        }

        if let Some(checksum,) = &self.content_md5 {
            headers.push((
                String::from("Content-MD5"), 
                checksum.to_owned()
            ));
        }
        
        if let Some((key, value)) = self.get_range_header() {
            headers.push((
                key, 
                value
            ));
        }

        headers
    }

    pub(crate) fn get_query(&self) -> Vec<(String, String)> {
        let mut query = Vec::new();
        if let Some(control) = self.get_cache_control_str() {
            query.push((
                String::from("response-cache-control"),
                control
            ));
        }

        if let Some(disposition) = self.get_content_disposition_str() {
            query.push((
                String::from("response-content-disposition"),
                disposition
            ));
        }

        if let Some(encoding) = self.get_content_encoding_str() {
            query.push((
                String::from("response-content-encoding"),
                encoding
            ));
        }

        if let Some(lang) = &self.content_language {
            query.push((
                String::from("response-content-language"),
                lang.to_owned()
            ));
        }

        if let Some(content_type) = &self.content_type {
            query.push((
                String::from("response-content-type"),
                content_type.to_owned()
            ));
        }

        if let Some(expires) = self.expires {
            query.push((
                String::from("response-expires"),
                expires.format("%A, %d %b %Y %H:%M:%S GMT").to_string()
            ));
        }

        query
    }
}