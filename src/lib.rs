pub mod config;
pub mod ring_consumer;
pub mod tracer;

pub use config::MonadConsumerPlugin;
pub use ring_consumer::{MonadConsumer, PluginConfig};
pub use tracer::FirehosePlugin;

pub use firehose::pb::sf::ethereum::r#type::v2::{Block, BlockHeader, TransactionTrace};

pub const TRACER_VERSION: &str = "1.0";
pub const TRACER_NAME: &str = "monad-firehose-tracer";
