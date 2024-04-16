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
  RuneDebited {
    rune_id: RuneId,
    block_height: u32,
    prev_outpoint: OutPoint,
    from: Address<NetworkUnchecked>,
    amount: u128,
    txid: Txid
  },
  RuneCredited {
    rune_id: RuneId,
    block_height: u32,
    outpoint: OutPoint,
    to: Address<NetworkUnchecked>,
    amount: u128,
    txid: Txid
  }
}
