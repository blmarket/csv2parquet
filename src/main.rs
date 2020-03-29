#![feature(test)]

use std::iter::FromIterator;
use std::io;

use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
// use tokio::stream::StreamExt;
use futures::Stream;
use futures_util::StreamExt;

use csv::ByteRecord;

mod wrong_column_writer;
mod row_group;
mod writer;

async fn async_read_raw(fname: &str) -> impl Stream<Item = io::Result<Vec<u8>>> {
    let file = File::open(fname).await.expect("Expects file");
    let reader = BufReader::with_capacity(1 << 20, file);
    reader.split(b'\n')
}

async fn async_read_record(fname: &str) -> impl Stream<Item = ByteRecord> {
    let file = File::open(fname).await.expect("Expects file");
    let reader = BufReader::with_capacity(1 << 20, file);
    reader.split(b'\n')
        .map(|line| ByteRecord::from_iter(line.expect("Should be a line").split(|b| *b == b'\t')))
}

async fn count_async_stream<T: Stream>(stream: T) -> Result<i32, Box<dyn std::error::Error>> {
    let mut line_count: i32 = 0;
    {
        let fut = stream.for_each(|_| {
            line_count += 1;
            futures::future::ready(())
        });
        fut.await;
    }
    Ok(line_count)
}

fn sync_read_record(fname: &str) -> impl Iterator<Item = usize> {
    use std::{fs, io::BufRead};
    let file = fs::File::open(fname).expect("Expects file");
    let reader = io::BufReader::new(file);
    reader.split(b'\n').map(|l| l.unwrap()).map(|line| {
        line.split(|b| *b == b'\t').map(|v| v.len()).sum()
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fname = "small";
    {
        let now = std::time::Instant::now();
        let async_stream = async_read_record(fname).await;
        println!("Read to ByteRecord: {}", count_async_stream(async_stream).await.expect("Expects count result"));
        println!("{}", now.elapsed().as_millis());
    }
    {
        let now = std::time::Instant::now();
        let async_stream = async_read_raw(fname).await;
        println!("Read raw: {}", count_async_stream(async_stream).await.expect("Expects count result"));
        println!("{}", now.elapsed().as_millis());
    }
    {
        let now = std::time::Instant::now();
        let async_stream = async_read_raw(fname).await;
        let async_stream2 = async_stream.map(|l| l.unwrap()).map(|line| line.split(|v| *v == b'\t').count());
        println!("Read raw then split: {}", count_async_stream(async_stream2).await.expect("Expects count result"));
        println!("{}", now.elapsed().as_millis());
    }
    {
        let now = std::time::Instant::now();
        let iterator = sync_read_record(fname);
        println!("Sync read: {}", iterator.count());
        println!("{}", now.elapsed().as_millis());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    extern crate test;

    #[test]
    fn test_schema() {
        use parquet::schema::parser;
        let message = "message schema {
            OPTIONAL BYTE_ARRAY snapshot_day (UTF8);
            OPTIONAL INT64 region_id (INT_64);
            OPTIONAL INT64 marketplace_id (INT_64);
            OPTIONAL INT64 merchant_customer_id (INT_64);
            OPTIONAL BYTE_ARRAY vendor_code (UTF8);
            OPTIONAL BYTE_ARRAY merchant_sku (UTF8);
            OPTIONAL BYTE_ARRAY asin (UTF8);
            OPTIONAL BYTE_ARRAY promotion_type (UTF8);
            OPTIONAL DOUBLE deal_price;
            OPTIONAL BYTE_ARRAY ineligibility (UTF8);
        }";
        parser::parse_message_type(message).expect("Expected valid schema");
    }
}
