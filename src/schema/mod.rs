use lru::LruCache;
use schema_registry_converter::async_impl::avro::AvroDecoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;

use crate::config::SchemaRegistryConfig;
use crate::error::DecodeError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadFormat {
    Avro,
    Json,
    Raw,
    Hex,
}

#[derive(Debug, Clone)]
pub struct DecodedMessage {
    pub format: PayloadFormat,
    pub json: String,
    pub schema_id: Option<u32>,
}

pub struct SchemaService {
    decoder: AvroDecoder<'static>,
    cache: LruCache<u32, String>,
}

impl SchemaService {
    pub fn new(config: &SchemaRegistryConfig) -> Self {
        let mut builder = SrSettings::new_builder(config.url.clone());
        if let Some(username) = config
            .properties
            .get("username")
            .or_else(|| config.properties.get("basic.auth.user.info"))
        {
            let password = config
                .properties
                .get("password")
                .map(|s| s.as_str());
            builder.set_basic_authorization(username, password);
        }
        for (k, v) in &config.properties {
            if !matches!(k.as_str(), "username" | "password" | "basic.auth.user.info") {
                builder.add_header(k, v);
            }
        }
        let sr_settings = builder.build().expect("invalid schema registry settings");
        let decoder = AvroDecoder::new(sr_settings);
        Self {
            decoder,
            cache: LruCache::new(std::num::NonZeroUsize::new(100).unwrap()),
        }
    }

    pub fn is_confluent_avro(payload: &[u8]) -> bool {
        payload.len() >= 5 && payload[0] == 0
    }

    pub fn schema_id(payload: &[u8]) -> Option<u32> {
        if Self::is_confluent_avro(payload) {
            Some(u32::from_be_bytes(payload[1..5].try_into().ok()?))
        } else {
            None
        }
    }

    pub async fn decode_value(
        &mut self,
        payload: &[u8],
    ) -> Result<DecodedMessage, DecodeError> {
        if !Self::is_confluent_avro(payload) {
            return Err(DecodeError::NotAvro);
        }

        let schema_id = u32::from_be_bytes(
            payload[1..5]
                .try_into()
                .map_err(|_| DecodeError::Decode("invalid schema id".into()))?,
        );

        let result = self
            .decoder
            .decode(Some(payload))
            .await
            .map_err(|e| DecodeError::SchemaRegistry(e.to_string()))?;

        let json = format!("{:#?}", result.value);

        self.cache.put(schema_id, json.clone());

        Ok(DecodedMessage {
            format: PayloadFormat::Avro,
            json,
            schema_id: Some(schema_id),
        })
    }
}
