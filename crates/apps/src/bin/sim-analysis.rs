#[tokio::main]
async fn main() -> anyhow::Result<()> {
    apps::sim_analysis::run().await
}
