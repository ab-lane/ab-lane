use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct PublisherEnvelope {
    pub topic_name: String,
    pub payload: Vec<u8>
}

pub trait Broker {
    fn new() -> BrokerNode;
    fn publish(&mut self, envelope: PublisherEnvelope) -> bool;
    fn subscribe(&mut self, topic_name: String, consumer_group: String) -> Option<Vec<u8>>;
}

pub struct BrokerNode {
    storage: Arc<Mutex<HashMap<String, BTreeMap<u128, Vec<u8>>>>>,
    offsets: Arc<Mutex<HashMap<String, u128>>>
}

impl Broker for BrokerNode {
    fn new() -> BrokerNode {
        BrokerNode {
            storage: Arc::new(Mutex::new(HashMap::new())),
            offsets: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    fn publish(&mut self, envelope: PublisherEnvelope) -> bool {
        let mut storage = self.storage.lock().unwrap();
        let offset = get_current_offset();
        
        if (*storage).contains_key(&envelope.topic_name) {
            if let Some(value) = (*storage).get_mut(&envelope.topic_name) {
                (*value).insert(offset, envelope.payload);
            }
        } else {
            let mut map = BTreeMap::new();
            map.insert(offset, envelope.payload);
            (*storage).insert(envelope.topic_name, map);
        }

        true
    }

    fn subscribe(&mut self, topic_name: String, consumer_group: String) -> Option<Vec<u8>> {
        let storage = self.storage.lock().unwrap();
        let mut offsets = self.offsets.lock().unwrap();

        let last_read_offset =
            if (*offsets).contains_key(&consumer_group) {
                (*offsets)[&consumer_group].to_owned()
            } else {
                (*offsets).insert(consumer_group.to_owned(), 0);
                0
            };

        match (*storage).get(&topic_name) {
            Some(messages) => {
                let current_offset = get_current_offset();
                match messages.range((Excluded(&last_read_offset), Included(&current_offset))).next() {
                    Some((offset, message)) => {
                        (*offsets).insert(consumer_group, *offset);
                        Some(message.to_owned())
                    },
                    None => None,
                }
            },
            None => None,
        }
    }
}

fn get_current_offset() -> u128 {
    let start = SystemTime::now();
    start.duration_since(UNIX_EPOCH).expect("Time went backwards").as_nanos()
}