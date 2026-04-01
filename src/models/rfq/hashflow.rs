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
) -> Result<HashMap<Bytes, Token>, Box<dyn Error>> {
    let file = File::open(filename)?;
    let mut reader = csv::Reader::from_reader(file);

    let mut map = HashMap::<Bytes, Token>::new();

    for result in reader.deserialize() {
        let record: Record = result?;

        if record.chain_id == 1 {
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
                chain: Chain::Ethereum,
                quality: Default::default(),
            };
            map.insert(address, token);
        }
    }

    Ok(map)
}
