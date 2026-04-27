use clap::Parser;
use color_eyre::eyre::Result;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use monad_firehose_tracer::{FirehosePlugin, MonadConsumer, MonadConsumerPlugin, PluginConfig};

#[derive(Parser, Debug)]
#[command(name = "monad-firehose-tracer")]
#[command(about = "Ethereum Firehose tracer for Monad blockchain")]
#[command(version = "0.14.0")]
struct Args {
    #[arg(long, default_value = "1")]
    chain_id: u64,

    #[arg(long, default_value = "monad")]
    network_name: String,

    #[arg(
        long,
        env = "MONAD_EVENT_RING_PATH",
        default_value = "/tmp/monad_events"
    )]
    monad_event_ring_path: String,

    #[arg(long, default_value = "524288")]
    event_channel_buffer_size: usize,

    #[arg(long, env = "DEBUG")]
    debug: bool,

    #[arg(long)]
    no_op: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let args = Args::parse();

    let level = if args.debug { Level::DEBUG } else { Level::INFO };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Monad Firehose tracer");
    info!("Chain ID: {}", args.chain_id);
    info!("Network: {}", args.network_name);
    info!("Event ring path: {}", args.monad_event_ring_path);
    info!("Debug mode: {}", args.debug);

    let consumer_config = PluginConfig {
        event_ring_path: args.monad_event_ring_path,
        event_channel_buffer_size: args.event_channel_buffer_size,
    };

    let consumer = MonadConsumer::new(consumer_config).await?;

    let tracer_config = MonadConsumerPlugin::new(args.chain_id)
        .with_debug(args.debug)
        .with_event_channel_buffer_size(args.event_channel_buffer_size)
        .with_no_op(args.no_op);

    let mut tracer = FirehosePlugin::new(tracer_config).with_consumer(consumer);

    if args.no_op {
        info!("NO-OP MODE ENABLED: Only logging block numbers, no processing");
    }

    info!("Starting Firehose tracer...");
    tracer.start().await?;

    Ok(())
}
