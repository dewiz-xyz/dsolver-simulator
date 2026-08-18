pub async fn signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    let _ = result;
                }
                signal = terminate.recv() => {
                    let _ = signal;
                }
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
