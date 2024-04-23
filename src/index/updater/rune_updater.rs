use super::*;

pub(super) struct RuneUpdater<'a, 'tx, 'client> {
  pub(super) block_time: u32,
  pub(super) block_hash: BlockHash,
  pub(super) burned: HashMap<RuneId, Lot>,
  pub(super) client: &'client Client,
  pub(super) event_sender: Option<&'a Sender<Event>>,
  pub(super) height: u32,
  pub(super) id_to_entry: &'a mut Table<'tx, RuneIdValue, RuneEntryValue>,
  pub(super) inscription_id_to_sequence_number: &'a Table<'tx, InscriptionIdValue, u32>,
  pub(super) minimum: Rune,
  pub(super) outpoint_to_balances: &'a mut Table<'tx, &'static OutPointValue, &'static [u8]>,
  pub(super) outpoint_to_owner: &'a mut Table<'tx, &'static OutPointValue, &'static [u8]>,
  pub(super) rune_to_id: &'a mut Table<'tx, u128, RuneIdValue>,
  pub(super) runes: u64,
  pub(super) sequence_number_to_rune_id: &'a mut Table<'tx, u32, RuneIdValue>,
  pub(super) statistic_to_count: &'a mut Table<'tx, u64, u64>,
  pub(super) transaction_id_to_rune: &'a mut Table<'tx, &'static TxidValue, u128>,
  pub(super) network: Network,
  // keep track on rune events
  pub(super) event_count: u32
}

impl<'a, 'tx, 'client> RuneUpdater<'a, 'tx, 'client> {
  pub(super) fn index_runes(&mut self, tx_index: u32, tx: &Transaction, txid: Txid) -> Result<()> {
    
    let mut unallocated = self.unallocated(txid, tx_index, tx)?;
    let artifact = Runestone::decipher(tx);

    // break early if nothing to index
    if let Some(Artifact::Cenotaph(cenotaph)) = &artifact {
      if cenotaph.etching.is_none() && cenotaph.mint.is_none() && unallocated.keys().len() == 0 {
        println!("Tx {} no runes to index", tx_index);
        return Ok(());
      }
    }
    
    let address_debit_balance = self.get_address_balance(&unallocated);
    let mut address_credit_balance: HashMap<String, HashMap<RuneId, Lot>> = HashMap::new();
    let mut rune_ids = unallocated.keys().cloned().collect::<Vec<RuneId>>();

    let mut allocated: Vec<HashMap<RuneId, Lot>> = vec![HashMap::new(); tx.output.len()];

    let output_address = |vout: usize| -> Option<Address> {
      let script_pubkey = &tx.output[vout].script_pubkey;
      let address = Address::from_script(&script_pubkey, self.network).ok();
      address
    };

    let mut output_addresses: HashMap<usize, String> = HashMap::new();
    for (vout, address) in tx.output.iter().enumerate().filter_map(|(vout, _)| {
      output_address(vout).map(|address| (vout, address.to_string()))
    }) {
      output_addresses.insert(vout, address);
    }
    
    if let Some(artifact) = &artifact {

      let start = Instant::now();
      
      if let Some(id) = artifact.mint() {
        if let Some(amount) = self.mint(id)? {
          unallocated.entry(id).or_default().push((amount, None));
          
          let event_send_start = Instant::now();

          if let Some(sender) = self.event_sender {
            match sender.try_send(Event::RuneMinted {
              block_height: self.height,
              tx_index,
              block_hash: self.block_hash,
              txid,
              rune_id: id,
              amount: amount.n(),
            }) {
              Ok(_) => {},
              Err(e) => {
                println!("Error sending event: {:?}", e);
              }
            };
            self.event_count += 1;
          }

          let event_send_duration = event_send_start.elapsed();
          println!("Event send duration: {:?}", event_send_duration);
        }
      }

      let etched = self.etched(tx_index, tx, artifact)?;

      if let Artifact::Runestone(runestone) = artifact {
        if let Some((id, ..)) = etched {
          unallocated.entry(id).or_default().push(
            (Lot(runestone.etching.unwrap().premine.unwrap_or_default()), None)
          );
        }

        for Edict { id, amount, output } in runestone.edicts.iter().copied() {
          let amount = Lot(amount);

          // edicts with output values greater than the number of outputs
          // should never be produced by the edict parser
          let output = usize::try_from(output).unwrap();
          assert!(output <= tx.output.len());

          let id = if id == RuneId::default() {
            let Some((id, ..)) = etched else {
              continue;
            };

            id
          } else {
            id
          };

          let Some(balance_items) = unallocated.get_mut(&id) else {
            continue;
          };
          let balance = &mut Lot(balance_items.iter().map(|(balance, _)| balance.0).sum::<u128>());
          let original_balance = balance.clone();

          let mut allocate = |balance: &mut Lot, amount: Lot, output: usize| {
            if amount > 0 {
              *balance -= amount;
              *allocated[output].entry(id).or_default() += amount;
            }
          };

          if output == tx.output.len() {
            // find non-OP_RETURN outputs
            let destinations = tx
              .output
              .iter()
              .enumerate()
              .filter_map(|(output, tx_out)| {
                (!tx_out.script_pubkey.is_op_return()).then_some(output)
              })
              .collect::<Vec<usize>>();

            if !destinations.is_empty() {
              if amount == 0 {
                // if amount is zero, divide balance between eligible outputs
                let amount = *balance / destinations.len() as u128;
                let remainder = usize::try_from(*balance % destinations.len() as u128).unwrap();

                for (i, output) in destinations.iter().enumerate() {
                  allocate(
                    balance,
                    if i < remainder { amount + 1 } else { amount },
                    *output,
                  );
                }
              } else {
                // if amount is non-zero, distribute amount to eligible outputs
                for output in destinations {
                  allocate(balance, amount.min(*balance), output);
                }
              }
            }
          } else {
            // Get the allocatable amount
            let amount = if amount == 0 {
              *balance
            } else {
              amount.min(*balance)
            };
            allocate(balance, amount, output);
          }

          // for the consumed balance, adjust the balance_items
          let used_balance = &mut (original_balance - *balance);
          
          for (balance, _) in balance_items.iter_mut() {
            if *balance > *used_balance {
              *balance -= *used_balance;
              break;
            } else {
              *used_balance -= *balance;
              *balance = Lot(0);
            }
          }

          // remove empty balance items
          balance_items.retain(|(balance, _)| balance.0 > 0);
        }
      }

      if let Some((id, rune)) = etched {
        self.create_rune_entry(txid, artifact, id, rune)?;
      }

      // let etched_time = start.elapsed();
      // println!("parse etch and edict time: {:?}", etched_time);
    }

    let mut burned: HashMap<RuneId, Lot> = HashMap::new();

    if let Some(Artifact::Cenotaph(_)) = artifact {
      for (id, balance_items) in unallocated {
        let balance = balance_items.iter().map(|(balance, _)| balance.0).sum::<u128>();
        *burned.entry(id).or_default() += balance;
      }
    } else {
      let pointer = artifact
        .map(|artifact| match artifact {
          Artifact::Runestone(runestone) => runestone.pointer,
          Artifact::Cenotaph(_) => unreachable!(),
        })
        .unwrap_or_default();

      // assign all un-allocated runes to the default output, or the first non
      // OP_RETURN output if there is no default, or if the default output is
      // too large
      if let Some(vout) = pointer
        .map(|pointer| pointer.into_usize())
        .inspect(|&pointer| assert!(pointer < allocated.len()))
        .or_else(|| {
          tx.output
            .iter()
            .enumerate()
            .find(|(_vout, tx_out)| !tx_out.script_pubkey.is_op_return())
            .map(|(vout, _tx_out)| vout)
        })
      {
        for (id, balance_items) in unallocated {
          let balance = balance_items.iter().map(|(balance, _)| balance.0).sum::<u128>();
          
          if balance > 0 {
            *allocated[vout].entry(id).or_default() += balance;
          }
        }
      } else {
        for (id, balance_items) in unallocated {
          let balance = balance_items.iter().map(|(balance, _)| balance.0).sum::<u128>();
          if balance > 0 {
            *burned.entry(id).or_default() += balance;
          }
        }
      }
    }

    let save_balance_start = Instant::now();

    // update outpoint balances
    let mut buffer: Vec<u8> = Vec::new();
    for (vout, balances) in allocated.into_iter().enumerate() {
      if balances.is_empty() {
        continue;
      }

      // increment burned balances
      if tx.output[vout].script_pubkey.is_op_return() {
        for (id, balance) in &balances {
          *burned.entry(*id).or_default() += *balance;
        }
        continue;
      }

      buffer.clear();

      let default = String::from("");
      let address_string = output_addresses.get(&vout).unwrap_or(&default);
      
      let mut balances = balances.into_iter().collect::<Vec<(RuneId, Lot)>>();

      // Sort balances by id so tests can assert balances in a fixed order
      balances.sort();

      let outpoint = OutPoint {
        txid,
        vout: vout.try_into().unwrap(),
      };
      
      if let Some(sender) = self.event_sender {
        sender.blocking_send(Event::RuneUtxoCreated {
          outpoint,
          block_height: self.height,
          block_hash: self.block_hash,
          tx_index,
          to: address_string.clone(),
          txid
        })?;
        self.event_count += 1;
      }

      for (id, balance) in balances {
        rune_ids.push(id);
        *address_credit_balance.entry(address_string.clone()).or_default().entry(id).or_default() += balance;
        Index::encode_rune_balance(id, balance.n(), &mut buffer);
      }

      self
        .outpoint_to_balances
        .insert(&outpoint.store(), buffer.as_slice())?;

      self
        .outpoint_to_owner
        .insert(&outpoint.store(),address_string.as_bytes())?;
    }

    // let save_balance_time = save_balance_start.elapsed();
    // println!("save balance time: {:?}", save_balance_time);
    

    // emit transfer related events

    if let Some(sender) = self.event_sender {
      // get unique rune ids
      let unique_rune_ids: HashSet<RuneId> = rune_ids.into_iter().collect();
      let unique_addresses: HashSet<String> = address_debit_balance.clone().into_keys().chain(address_credit_balance.clone().into_keys()).collect();
      
      unique_addresses.into_iter().for_each(|address| {
        unique_rune_ids.iter().for_each(|id| {
          let debit = address_debit_balance.get(&address).and_then(|rune_balance| rune_balance.get(&id).cloned()).unwrap_or_default();
          let credit = address_credit_balance.get(&address).and_then(|rune_balance| rune_balance.get(&id).cloned()).unwrap_or_default();
          if debit > credit {
            // better handle error here
            sender.blocking_send(Event::RuneDebited {
              amount: (debit - credit).n(),
              block_height: self.height,
              tx_index,
              block_hash: self.block_hash,
              from: address.clone(),
              rune_id: id.clone(),
              txid
            }).unwrap();
            self.event_count += 1;
          } else {
            sender.blocking_send(Event::RuneCredited {
              amount: (credit - debit).n(),
              block_height: self.height,
              tx_index,
              block_hash: self.block_hash,
              rune_id: id.clone(),
              to: address.clone(),
              txid
            }).unwrap();
            self.event_count += 1;
          }
        });  
      });
    };

    // increment entries with burned runes
    for (id, amount) in burned {
      *self.burned.entry(id).or_default() += amount;

      if let Some(sender) = self.event_sender {
        sender.blocking_send(Event::RuneBurned {
          block_height: self.height,
          tx_index,
          block_hash: self.block_hash,
          txid,
          rune_id: id,
          amount: amount.n(),
        })?;
        self.event_count += 1;
      }
    }

    Ok(())
  }

  pub(super) fn update(self) -> Result {
    for (rune_id, burned) in self.burned {
      let mut entry = RuneEntry::load(self.id_to_entry.get(&rune_id.store())?.unwrap().value());
      entry.burned = entry.burned.checked_add(burned.n()).unwrap();
      self.id_to_entry.insert(&rune_id.store(), entry.store())?;
    }

    Ok(())
  }

  fn create_rune_entry(
    &mut self,
    txid: Txid,
    artifact: &Artifact,
    id: RuneId,
    rune: Rune,
  ) -> Result {
    self.rune_to_id.insert(rune.store(), id.store())?;
    self
      .transaction_id_to_rune
      .insert(&txid.store(), rune.store())?;

    let number = self.runes;
    self.runes += 1;

    self
      .statistic_to_count
      .insert(&Statistic::Runes.into(), self.runes)?;

    let entry = match artifact {
      Artifact::Cenotaph(_) => RuneEntry {
        block: id.block,
        burned: 0,
        divisibility: 0,
        etching: txid,
        terms: None,
        mints: 0,
        number,
        premine: 0,
        spaced_rune: SpacedRune { rune, spacers: 0 },
        symbol: None,
        timestamp: self.block_time.into(),
        turbo: false,
      },
      Artifact::Runestone(Runestone { etching, .. }) => {
        let Etching {
          divisibility,
          terms,
          premine,
          spacers,
          symbol,
          turbo,
          ..
        } = etching.unwrap();

        RuneEntry {
          block: id.block,
          burned: 0,
          divisibility: divisibility.unwrap_or_default(),
          etching: txid,
          terms,
          mints: 0,
          number,
          premine: premine.unwrap_or_default(),
          spaced_rune: SpacedRune {
            rune,
            spacers: spacers.unwrap_or_default(),
          },
          symbol,
          timestamp: self.block_time.into(),
          turbo,
        }
      }
    };

    self.id_to_entry.insert(id.store(), entry.store())?;

    if let Some(sender) = self.event_sender {
      sender.blocking_send(Event::RuneEtched {
        block_height: self.height,
        tx_index: id.tx,
        block_hash: self.block_hash,
        txid,
        divisibility: entry.divisibility,
        number,
        premine: entry.premine,
        spaced_rune: entry.spaced_rune.to_string(),
        symbol: entry.symbol.clone().unwrap_or_default(),
        turbo: entry.turbo,
        amount: entry.terms.and_then(|t| t.amount),
        cap: entry.terms.and_then(|t| t.cap),
        height_start: entry.terms.and_then(|t| t.height.0),
        height_end: entry.terms.and_then(|t| t.height.1),
        offset_start: entry.terms.and_then(|t| t.offset.0),
        offset_end: entry.terms.and_then(|t| t.offset.1)
      })?;
      self.event_count += 1;
    }

    let inscription_id = InscriptionId { txid, index: 0 };

    if let Some(sequence_number) = self
      .inscription_id_to_sequence_number
      .get(&inscription_id.store())?
    {
      self
        .sequence_number_to_rune_id
        .insert(sequence_number.value(), id.store())?;
    }

    Ok(())
  }

  fn etched(
    &mut self,
    tx_index: u32,
    tx: &Transaction,
    artifact: &Artifact,
  ) -> Result<Option<(RuneId, Rune)>> {
    let rune = match artifact {
      Artifact::Runestone(runestone) => match runestone.etching {
        Some(etching) => etching.rune,
        None => return Ok(None),
      },
      Artifact::Cenotaph(cenotaph) => match cenotaph.etching {
        Some(rune) => Some(rune),
        None => return Ok(None),
      },
    };

    let rune = if let Some(rune) = rune {
      if rune < self.minimum
        || rune.is_reserved()
        || self.rune_to_id.get(rune.0)?.is_some()
        || !self.tx_commits_to_rune(tx, rune)?
      {
        return Ok(None);
      }
      rune
    } else {
      let reserved_runes = self
        .statistic_to_count
        .get(&Statistic::ReservedRunes.into())?
        .map(|entry| entry.value())
        .unwrap_or_default();

      self
        .statistic_to_count
        .insert(&Statistic::ReservedRunes.into(), reserved_runes + 1)?;

      Rune::reserved(self.height.into(), tx_index)
    };

    Ok(Some((
      RuneId {
        block: self.height.into(),
        tx: tx_index,
      },
      rune,
    )))
  }

  fn mint(&mut self, id: RuneId) -> Result<Option<Lot>> {
    let Some(entry) = self.id_to_entry.get(&id.store())? else {
      return Ok(None);
    };

    let mut rune_entry = RuneEntry::load(entry.value());

    let Ok(amount) = rune_entry.mintable(self.height.into()) else {
      return Ok(None);
    };

    drop(entry);

    rune_entry.mints += 1;

    self.id_to_entry.insert(&id.store(), rune_entry.store())?;

    Ok(Some(Lot(amount)))
  }

  fn tx_commits_to_rune(&self, tx: &Transaction, rune: Rune) -> Result<bool> {
    let commitment = rune.commitment();

    for input in &tx.input {
      // extracting a tapscript does not indicate that the input being spent
      // was actually a taproot output. this is checked below, when we load the
      // output's entry from the database
      let Some(tapscript) = input.witness.tapscript() else {
        continue;
      };

      for instruction in tapscript.instructions() {
        // ignore errors, since the extracted script may not be valid
        let Ok(instruction) = instruction else {
          break;
        };

        let Some(pushbytes) = instruction.push_bytes() else {
          continue;
        };

        if pushbytes.as_bytes() != commitment {
          continue;
        }

        let Some(tx_info) = self
          .client
          .get_raw_transaction_info(&input.previous_output.txid, None)
          .into_option()?
        else {
          panic!(
            "can't get input transaction: {}",
            input.previous_output.txid
          );
        };

        let taproot = tx_info.vout[input.previous_output.vout.into_usize()]
          .script_pub_key
          .script()?
          .is_v1_p2tr();

        if !taproot {
          continue;
        }

        let commit_tx_height = self
          .client
          .get_block_header_info(&tx_info.blockhash.unwrap())
          .into_option()?
          .unwrap()
          .height;

        let confirmations = self
          .height
          .checked_sub(commit_tx_height.try_into().unwrap())
          .unwrap()
          + 1;

        if confirmations >= Runestone::COMMIT_CONFIRMATIONS.into() {
          return Ok(true);
        }
      }
    }

    Ok(false)
  }

  fn get_address_balance(&mut self, unallocated: &HashMap<RuneId, Vec<(Lot, Option<String>)>>) -> HashMap<String, HashMap<RuneId, Lot>> {
    let mut address_balance: HashMap<String, HashMap<RuneId, Lot>> = HashMap::new();
    
    for (id, balance_items) in unallocated {
      for (balance, address) in balance_items {
        if let Some(address) = address {
          *address_balance.entry(address.clone()).or_default().entry(id.clone()).or_default() += balance.clone();
        }
      }
    }

    address_balance
  }

  fn unallocated(&mut self, txid: Txid, tx_index: u32, tx: &Transaction) -> Result<HashMap<RuneId, Vec<(Lot, Option<String>)>>> {
    // map of rune ID to un-allocated balance of that rune
    let mut unallocated: HashMap<RuneId, Vec<(Lot, Option<String>)>> = HashMap::new();

    // increment unallocated runes with the runes in tx inputs
    for input in &tx.input {
      
      if let Some(owner_guard) = self
        .outpoint_to_owner
        .remove(&input.previous_output.store())?
      {
        let owner_buffer = owner_guard.value();
        let address = String::from_utf8(owner_buffer.to_vec()).ok();
        
        // emit debit events
        if let Some(sender) = self.event_sender {
          if let Some(address) = &address {
            sender.blocking_send(Event::RuneUtxoSpent {
              block_height: self.height,
              tx_index,
              block_hash: self.block_hash,
              txid,
              from: address.clone(),
              prev_outpoint: input.previous_output
            })?;
          }
        }

        if let Some(guard) = self
          .outpoint_to_balances
          .remove(&input.previous_output.store())?
        {
          let buffer = guard.value();
          let mut i = 0;
          while i < buffer.len() {
            let ((id, balance), len) = Index::decode_rune_balance(&buffer[i..]).unwrap();
            i += len;
            let balance_item = (Lot(balance), address.clone());
            unallocated.entry(id).or_default().push(balance_item);
          }
        }
      }
    }

    Ok(unallocated)
  }
}
