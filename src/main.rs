#![feature(duration_as_u128)]

extern crate time;

mod broker;

use std::str;

use broker::*;

fn main() {
    let mut broker = BrokerNode::new();

    let topic = "hello_world";

    let consumer_group = "cheese";

    assert!(broker.publish(PublisherEnvelope{ topic_name: topic.to_owned(), payload: vec![240, 159, 146, 150]}));
    assert!(broker.publish(PublisherEnvelope{ topic_name: topic.to_owned(), payload: vec![240, 159, 146, 150]}));
    assert!(broker.publish(PublisherEnvelope{ topic_name: topic.to_owned(), payload: vec![240, 159, 146, 150]}));
    assert!(broker.publish(PublisherEnvelope{ topic_name: topic.to_owned(), payload: vec![240, 159, 146, 150]}));
    assert!(broker.publish(PublisherEnvelope{ topic_name: topic.to_owned(), payload: vec![240, 159, 146, 150]}));
    assert!(broker.publish(PublisherEnvelope{ topic_name: topic.to_owned(), payload: vec![240, 159, 146, 150]}));

    loop {
        match broker.subscribe(topic.to_owned(), consumer_group.to_owned()) {
            Some (message) => println!("{}", str::from_utf8(&message).unwrap()),
            None => break,
        }
    }
}
