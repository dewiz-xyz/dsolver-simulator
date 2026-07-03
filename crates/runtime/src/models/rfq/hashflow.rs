use std::{collections::HashMap, error::Error, fs::File, io, path::Path, str::FromStr};

use serde::Deserialize;
use tycho_simulation::tycho_common::{
    models::{token::Token, Chain},
    Bytes,
};

//evm,1,1INCH,1inch,0x111111111117dc0aa78b770fa6a738034120c302,18
#[derive(Deserialize)]
struct Record {
    #[expect(dead_code)]
    chain_type: String,
    chain_id: u32,
    name: String,
    #[expect(dead_code)]
    display_name: String,
    address: String,
    decimals: u32,
}

pub fn read_hashflow_csv<P: AsRef<Path>>(
    filename: P,
    chain: Chain,
) -> Result<HashMap<Bytes, Token>, Box<dyn Error>> {
    let file = File::open(filename)?;
    let mut reader = csv::Reader::from_reader(file);

    let mut map = HashMap::<Bytes, Token>::new();

    for result in reader.deserialize() {
        let record: Record = result?;

        // The shared CSV can contain supported tokens for multiple chains.
        if u64::from(record.chain_id) == chain.id() {
            let address = Bytes::from_str(record.address.as_str()).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid Hashflow contract address {}: {err}",
                        record.address
                    ),
                )
            })?;
            let token = Token {
                address: address.clone(),
                symbol: record.name,
                decimals: record.decimals,
                tax: 0,
                gas: vec![],
                chain,
                quality: Default::default(),
            };
            map.insert(address, token);
        }
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, time::UNIX_EPOCH};

    use super::*;

    const ETH_ADDRESS: &str = "0x1111111111111111111111111111111111111111";
    const BASE_ADDRESS: &str = "0x4200000000000000000000000000000000000006";

    struct CsvFixture {
        path: PathBuf,
    }

    impl CsvFixture {
        fn new(contents: &str) -> Result<Self, Box<dyn Error>> {
            let path = std::env::temp_dir().join(format!(
                "hashflow-supported-tokens-{}-{}.csv",
                std::process::id(),
                UNIX_EPOCH.elapsed()?.as_nanos()
            ));
            fs::write(&path, contents)?;
            Ok(Self { path })
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for CsvFixture {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn csv_fixture() -> Result<CsvFixture, Box<dyn Error>> {
        CsvFixture::new(&format!(
            "chain_type,chain_id,name,display_name,address,decimals\n\
             evm,1,WETH,Wrapped Ether,{ETH_ADDRESS},18\n\
             evm,8453,WETH,Wrapped Ether,{BASE_ADDRESS},18\n"
        ))
    }

    #[test]
    fn read_hashflow_csv_filters_for_ethereum() -> Result<(), Box<dyn Error>> {
        let fixture = csv_fixture()?;
        let tokens = read_hashflow_csv(fixture.path(), Chain::Ethereum)?;
        let address = Bytes::from_str(ETH_ADDRESS)?;
        let token = tokens
            .get(&address)
            .ok_or_else(|| io::Error::other("expected Ethereum token"))?;

        assert_eq!(tokens.len(), 1);
        assert_eq!(token.address, address);
        assert_eq!(token.symbol, "WETH");
        assert_eq!(token.decimals, 18);
        assert_eq!(token.chain, Chain::Ethereum);
        Ok(())
    }

    #[test]
    fn read_hashflow_csv_filters_for_base() -> Result<(), Box<dyn Error>> {
        let fixture = csv_fixture()?;
        let tokens = read_hashflow_csv(fixture.path(), Chain::Base)?;
        let address = Bytes::from_str(BASE_ADDRESS)?;
        let token = tokens
            .get(&address)
            .ok_or_else(|| io::Error::other("expected Base token"))?;

        assert_eq!(tokens.len(), 1);
        assert_eq!(token.address, address);
        assert_eq!(token.symbol, "WETH");
        assert_eq!(token.decimals, 18);
        assert_eq!(token.chain, Chain::Base);
        Ok(())
    }
}
