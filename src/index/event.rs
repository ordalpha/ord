use super::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Event {
  InscriptionCreated {
    block_height: u32,
    charms: u16,
    inscription_id: InscriptionId,
    location: Option<SatPoint>,
    parent_inscription_ids: Vec<InscriptionId>,
    sequence_number: u32,
  },
  InscriptionTransferred {
    block_height: u32,
    inscription_id: InscriptionId,
    new_location: SatPoint,
    old_location: SatPoint,
    sequence_number: u32,
  },
  RuneBurned {
    amount: u128,
    block_height: u32,
    rune_id: RuneId,
    txid: Txid,
  },
  RuneEtched {
    block_height: u32,
    rune_id: RuneId,
    txid: Txid,
  },
  RuneMinted {
    amount: u128,
    block_height: u32,
    rune_id: RuneId,
    txid: Txid
  },
  RuneUtxoSpent {
    block_height: u32,
    prev_outpoint: OutPoint,
    from: String,
    txid: Txid
  },
  RuneUtxoCreated {
    block_height: u32,
    outpoint: OutPoint,
    to: String,
    txid: Txid
  },
  RuneDebited {
    amount: u128,
    block_height: u32,
    from: String,
    rune_id: RuneId,
    txid: Txid
  },
  RuneCredited {
    amount: u128,
    block_height: u32,
    rune_id: RuneId,
    to: String,
    txid: Txid
  }
}
