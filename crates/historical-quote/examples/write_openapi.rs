use std::{env, error::Error, fs, io, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing output path"))?;
    let document = historical_quote::openapi::document_value();
    let json = serde_json::to_string_pretty(&document)?;
    fs::write(path, format!("{json}\n"))?;
    Ok(())
}
