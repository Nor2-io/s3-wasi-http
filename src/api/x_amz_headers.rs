use chrono::{DateTime, Utc};

pub enum XAmzCannedAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthRead,
    AWSExecRead,
    BucketOwnerRead,
    BucketOwnerFullControl,
    Acl(String),
}

pub enum XAmzChecksum {
    CRC32(String),
    CRC32C(String),
    CRC64NVME(String),
    SHA1(String),
    Sha256(String),
    Checksum(String, String),
}

pub enum XAmzGrants {
    FullControl,
    Read,
    ReadACP,
    WriteACP,
}

pub enum XAmzObjectLockMode {
    Governance,
    Compliance,
}

pub enum XAmzServerSideEncryption {
    AES256,
    KMS,
    KMSDSSE,
    Algorithm(String),
}

pub enum XAmzStorageClass {
    Standard,
    ReducedRedundancy,
    StandardIA,
    OnezoneIA,
    IntelligentTiering,
    Glacier,
    DeepArchive,
    Outposts,
    GlacierIR,
    Snow,
    ExpressOneZone,
    StorageClass(String),
}
pub(crate) fn storage_class_from_str(class: String) -> XAmzStorageClass {
    match class.to_lowercase() {
        c if c == "standard" => XAmzStorageClass::Standard,
        c if c == "reduced_redundancy" => XAmzStorageClass::ReducedRedundancy,
        c if c == "glacier" => XAmzStorageClass::Glacier,
        c if c == "standard_ia" => XAmzStorageClass::StandardIA,
        c if c == "onezone_ia" => XAmzStorageClass::OnezoneIA,
        c if c == "intelligent_tiering" => XAmzStorageClass::IntelligentTiering,
        c if c == "deep_archive" => XAmzStorageClass::DeepArchive,
        c if c == "outposts" => XAmzStorageClass::Outposts,
        c if c == "glacier_ir" => XAmzStorageClass::GlacierIR,
        c if c == "snow" => XAmzStorageClass::Snow,
        c if c == "express_onezone" => XAmzStorageClass::ExpressOneZone,

        c => XAmzStorageClass::StorageClass(c),
    }
}

/// Set x-amz headers on a request
///
/// see [super::S3RequestBuilder::set_x_amz_headers]
pub struct XAmzHeaders {
    pub(crate) headers: Vec<(String, String)>,
}

impl XAmzHeaders {
    pub(crate) fn headers(&self) -> Vec<(String, String)> {
        self.headers.clone()
    }
}

pub struct XAmzHeadersBuilder {
    checksum_mode: bool,
    expected_bucket_owner: Option<String>,
    request_payer: bool,

    encryption_customer_algorithm: Option<String>,
    encryption_customer_key: Option<String>,
    encryption_customer_key_md5: Option<String>,
    encryption_algorithm: Option<XAmzServerSideEncryption>,
    encryption_kms_key_id: Option<String>,
    encryption_bucket_key: bool,
    encryption_context: Option<String>,

    object_lock_legal_hold: bool,
    object_lock_mode: Option<XAmzObjectLockMode>,
    object_lock_retain_until: Option<DateTime<Utc>>,

    canned_acl: Option<XAmzCannedAcl>,
    checksum: Option<XAmzChecksum>,
    grants: Vec<XAmzGrants>,
    storage_class: Option<XAmzStorageClass>,

    tagging: Vec<(String, String)>,
    website_redirect_location: Option<String>,
    write_offset: Option<i32>,

    headers: Vec<(String, String)>,
}

impl XAmzHeadersBuilder {
    pub fn enable_checksum_mode(self) -> Self {
        Self {
            checksum_mode: true,
            ..self
        }
    }
    pub fn expected_bucket_owner(self, owner: &str) -> Self {
        Self {
            expected_bucket_owner: Some(owner.to_owned()),
            ..self
        }
    }
    pub fn enable_request_payer(self) -> Self {
        Self {
            request_payer: true,
            ..self
        }
    }

    pub fn encryption_customer_algorithm(self, algorithm: &str) -> Self {
        Self {
            encryption_customer_algorithm: Some(algorithm.to_owned()),
            ..self
        }
    }
    pub fn encryption_customer_key(self, base64_encoded_key: &str) -> Self {
        Self {
            encryption_customer_key: Some(base64_encoded_key.to_owned()),
            ..self
        }
    }
    pub fn encryption_customer_key_md5(self, base64_encoded_key_md5: &str) -> Self {
        Self {
            encryption_customer_key_md5: Some(base64_encoded_key_md5.to_owned()),
            ..self
        }
    }
    pub fn encryption_algorithm(self, algorithm: XAmzServerSideEncryption) -> Self {
        Self {
            encryption_algorithm: Some(algorithm),
            ..self
        }
    }
    pub fn encryption_kms_key_id(self, key_id: &str) -> Self {
        Self {
            encryption_kms_key_id: Some(key_id.to_owned()),
            ..self
        }
    }
    pub fn set_encryption_bucket_key(self) -> Self {
        Self {
            encryption_bucket_key: true,
            ..self
        }
    }
    pub fn encryption_context(self, context: String) -> Self {
        Self {
            encryption_context: Some(context),
            ..self
        }
    }

    pub fn set_object_lock_legal_hold(self) -> Self {
        Self {
            object_lock_legal_hold: true,
            ..self
        }
    }
    pub fn object_lock_mode(self, mode: XAmzObjectLockMode) -> Self {
        Self {
            object_lock_mode: Some(mode),
            ..self
        }
    }
    pub fn object_lock_retain_until(self, date: DateTime<Utc>) -> Self {
        Self {
            object_lock_retain_until: Some(date),
            ..self
        }
    }

    pub fn canned_acl(self, acl: XAmzCannedAcl) -> Self {
        Self {
            canned_acl: Some(acl),
            ..self
        }
    }
    pub fn checksum(self, checksum: XAmzChecksum) -> Self {
        Self {
            checksum: Some(checksum),
            ..self
        }
    }
    pub fn add_grant(self, grant: XAmzGrants) -> Self {
        let mut grants = self.grants;
        grants.push(grant);
        Self {
            grants: grants,
            ..self
        }
    }
    pub fn storage_class(self, class: XAmzStorageClass) -> Self {
        Self {
            storage_class: Some(class),
            ..self
        }
    }

    pub fn add_tag(self, key: &str, value: &str) -> Self {
        let mut tags = self.tagging;
        tags.push((key.to_owned(), value.to_owned()));
        Self {
            tagging: tags,
            ..self
        }
    }
    pub fn website_redirect_location(self, location: String) -> Self {
        Self {
            website_redirect_location: Some(location),
            ..self
        }
    }
    pub fn write_offset(self, bytes: i32) -> Self {
        Self {
            write_offset: Some(bytes),
            ..self
        }
    }

    pub fn add_header(self, key: &str, value: &str) -> Self {
        let mut headers = self.headers;
        headers.push((key.to_lowercase(), value.trim().to_owned()));
        Self { headers, ..self }
    }

    fn get_encryption_algorithm(&self) -> Option<String> {
        match &self.encryption_algorithm {
            Some(algorithm) => {
                let a = match algorithm {
                    XAmzServerSideEncryption::AES256 => "AES256",
                    XAmzServerSideEncryption::KMS => "aws:kms",
                    XAmzServerSideEncryption::KMSDSSE => "aws:kms:dsse",
                    XAmzServerSideEncryption::Algorithm(algo) => algo,
                };

                Some(a.to_owned())
            }
            None => None,
        }
    }
    fn get_canned_acl(&self) -> Option<String> {
        match &self.canned_acl {
            Some(acl) => {
                let a = match acl {
                    XAmzCannedAcl::Private => "private",
                    XAmzCannedAcl::PublicRead => "public-read",
                    XAmzCannedAcl::PublicReadWrite => "public-read-write",
                    XAmzCannedAcl::AuthRead => "authenticated-read",
                    XAmzCannedAcl::AWSExecRead => "aws-exec-read",
                    XAmzCannedAcl::BucketOwnerRead => "bucket-owner-read",
                    XAmzCannedAcl::BucketOwnerFullControl => "bucket-owner-full-control",
                    XAmzCannedAcl::Acl(str) => str,
                };

                Some(a.to_owned())
            }
            None => None,
        }
    }
    fn get_checksum_header(&self) -> Option<(String, String)> {
        match &self.checksum {
            Some(checksum) => {
                let (key, check) = match checksum {
                    XAmzChecksum::CRC32(sum) => ("crc32".to_string(), sum),
                    XAmzChecksum::CRC32C(sum) => ("crc32c".to_string(), sum),
                    XAmzChecksum::CRC64NVME(sum) => ("crc64nvme".to_string(), sum),
                    XAmzChecksum::SHA1(sum) => ("sha1".to_string(), sum),
                    XAmzChecksum::Sha256(sum) => ("sha256".to_string(), sum),
                    XAmzChecksum::Checksum(k, sum) => (k.to_owned(), sum),
                };

                Some((format!("x-amz-checksum-{key}"), check.to_owned()))
            }
            None => None,
        }
    }
    fn get_grants_headers(&self) -> Vec<(String, String)> {
        let mut headers = Vec::new();
        for grant in &self.grants {
            match grant {
                XAmzGrants::FullControl => {
                    headers.push(("x-amz-grant-full-control".to_string(), String::new()));
                }
                XAmzGrants::Read => {
                    headers.push(("x-amz-grant-read".to_string(), String::new()));
                }
                XAmzGrants::ReadACP => {
                    headers.push(("x-amz-grant-read-acp".to_string(), String::new()));
                }
                XAmzGrants::WriteACP => {
                    headers.push(("x-amz-grant-write-acp".to_string(), String::new()));
                }
            }
        }

        headers
    }
    fn get_storage_class(&self) -> Option<String> {
        match &self.storage_class {
            Some(class) => {
                let c = match class {
                    XAmzStorageClass::Standard => "STANDARD",
                    XAmzStorageClass::ReducedRedundancy => "REDUCED_REDUNDANCY",
                    XAmzStorageClass::StandardIA => "STANDARD_IA",
                    XAmzStorageClass::OnezoneIA => "ONEZONE_IA",
                    XAmzStorageClass::IntelligentTiering => "INTELLIGENT_TIERING",
                    XAmzStorageClass::Glacier => "GLACIER",
                    XAmzStorageClass::DeepArchive => "DEEP_ARCHIVE",
                    XAmzStorageClass::Outposts => "OUTPOSTS",
                    XAmzStorageClass::GlacierIR => "GLACIER_IR",
                    XAmzStorageClass::Snow => "SNOW",
                    XAmzStorageClass::ExpressOneZone => "EXPRESS_ONEZONE",
                    XAmzStorageClass::StorageClass(class_str) => &class_str,
                };

                Some(c.to_owned())
            }
            None => None,
        }
    }
    fn get_tagging(&self) -> Option<String> {
        if self.tagging.is_empty() {
            return None;
        }

        Some(
            self.tagging
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<String>>()
                .join("&"),
        )
    }

    pub fn build(&self) -> XAmzHeaders {
        let mut headers = Vec::new();
        if self.checksum_mode {
            headers.push(("x-amz-checksum-mode".to_string(), "ENABLED".to_string()));
        }
        if let Some(owner) = &self.expected_bucket_owner {
            headers.push(("x-amz-expected-bucket-owner".to_string(), owner.to_owned()));
        }
        if self.request_payer {
            headers.push(("x-amz-request-payer".to_string(), "requester".to_string()));
        }

        if let Some(algorithm) = &self.encryption_customer_algorithm {
            headers.push((
                "x-amz-server-side-encryption-customer-algorithm".to_string(),
                algorithm.to_owned(),
            ));
        }
        if let Some(key) = &self.encryption_customer_key {
            headers.push((
                "x-amz-server-side-encryption-customer-key".to_string(),
                key.to_owned(),
            ));
        }
        if let Some(md5) = &self.encryption_customer_key_md5 {
            headers.push((
                "x-amz-server-side-encryption-customer-key-MD5".to_string(),
                md5.to_owned(),
            ));
        }
        if let Some(algorithm) = self.get_encryption_algorithm() {
            headers.push(("x-amz-server-side-encryption".to_string(), algorithm));
        }
        if let Some(key) = &self.encryption_kms_key_id {
            headers.push((
                "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
                key.to_owned(),
            ));
        }
        if self.encryption_bucket_key {
            headers.push((
                "x-amz-server-side-encryption-bucket-key-enabled".to_string(),
                "true".to_string(),
            ));
        }
        if let Some(context) = &self.encryption_context {
            headers.push((
                "x-amz-server-side-encryption-context".to_string(),
                context.to_owned(),
            ));
        }

        if let Some(acl) = self.get_canned_acl() {
            headers.push(("x-amz-acl".to_string(), acl));
        }
        if let Some((key, value)) = self.get_checksum_header() {
            headers.push((key, value));
        }
        let grants = self.get_grants_headers();
        for (key, value) in grants {
            headers.push((key, value));
        }
        if let Some(class) = self.get_storage_class() {
            headers.push(("x-amz-storage-class".to_string(), class));
        }

        if let Some(tagging) = self.get_tagging() {
            headers.push(("x-amz-tagging".to_string(), tagging));
        }
        if let Some(redirect) = &self.website_redirect_location {
            headers.push((
                "x-amz-website-redirect-location".to_string(),
                redirect.to_owned(),
            ));
        }
        if let Some(offset) = self.write_offset {
            headers.push(("x-amz-write-offset-bytes".to_string(), offset.to_string()));
        }

        for (key, value) in &self.headers {
            headers.push((key.to_owned(), value.to_owned()));
        }

        XAmzHeaders { headers }
    }
}

impl Default for XAmzHeadersBuilder {
    fn default() -> Self {
        Self {
            checksum_mode: false,
            expected_bucket_owner: None,
            request_payer: false,

            encryption_customer_algorithm: None,
            encryption_customer_key: None,
            encryption_customer_key_md5: None,
            encryption_algorithm: None,
            encryption_kms_key_id: None,
            encryption_bucket_key: false,
            encryption_context: None,

            object_lock_legal_hold: false,
            object_lock_mode: None,
            object_lock_retain_until: None,

            canned_acl: None,
            checksum: None,
            grants: Vec::new(),
            storage_class: None,

            tagging: Vec::new(),
            website_redirect_location: None,
            write_offset: None,

            headers: Vec::new(),
        }
    }
}
