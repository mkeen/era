use pallas::network::{
    miniprotocols::{
        handshake, PROTOCOL_N2N_BLOCK_FETCH, PROTOCOL_N2N_CHAIN_SYNC, PROTOCOL_N2N_HANDSHAKE,
    },
    multiplexer,
};

pub struct Transport {
    pub channel2: multiplexer::AgentChannel,
    pub channel3: multiplexer::AgentChannel,
    pub version: handshake::VersionNumber,
}

impl Transport {
    pub fn setup(address: &str, magic: u64) -> Result<Self, crate::Error> {
        log::debug!("connecting muxer");
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let bearer = runtime
            .block_on(multiplexer::Bearer::connect_tcp(address))
            .unwrap();

        let mut plexer = multiplexer::Plexer::new(bearer);

        log::debug!("doing handshake");

        let handshake_channel = plexer.subscribe_client(PROTOCOL_N2N_HANDSHAKE);

        let versions = handshake::n2n::VersionTable::v6_and_above(magic);
        let mut client = handshake::Client::new(handshake_channel);

        let output = runtime
            .block_on(client.handshake(versions))
            .map_err(crate::Error::ouroboros)?;

        log::info!("handshake output: {:?}", output);

        let version = match output {
            handshake::Confirmation::Accepted(version, _) => Ok(version),
            _ => Err(crate::Error::ouroboros(
                "couldn't agree on handshake version",
            )),
        };

        Ok(Self {
            channel2: plexer.subscribe_client(PROTOCOL_N2N_CHAIN_SYNC),
            channel3: plexer.subscribe_client(PROTOCOL_N2N_BLOCK_FETCH),
            version: version.unwrap(),
        })
    }
}
