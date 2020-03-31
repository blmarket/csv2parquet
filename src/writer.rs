// use crate::row_group::BufferedRowGroupWriter;
use futures::stream;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncSeek, Stream, StreamExt, TryStream, TryStreamExt};
use parquet::{
    column::buf_writer::ColumnWriter,
    file::{
        metadata::{ColumnChunkMetaData, RowGroupMetaData},
        properties::{WriterProperties},
    },
    schema::types::{SchemaDescPtr, SchemaDescriptor, Type},
};
use parquet::data_type::ByteArray;
use std::error::Error;
use std::rc::Rc;
use parquet_format;
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use byteorder::{ByteOrder, LittleEndian};
use std::io::{self, Write, Seek, Read};
use std::io::prelude::*;

use crate::row_group::BufferedRowGroupWriter;

const FOOTER_SIZE: usize = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];
const ROW_GROUP_SIZE: usize = 1 << 20;

pub async fn write_parquet<'a, S, W>(
    mut sink: &mut W,
    stream: S,
    schema: Rc<Type>,
    props: Rc<WriterProperties>,
) -> Result<(), Box<dyn Error>>
where
    S: Stream<Item = Vec<&'a str>>,
    W: Write + Seek,
{
    // let header = stream::iter(vec![Ok(PARQUET_MAGIC)]);

    io::copy(&mut &PARQUET_MAGIC[..], &mut sink).unwrap();

    let schema_descr = Rc::new(SchemaDescriptor::new(schema.clone()));

    let mut writer = BufferedRowGroupWriter::new(&mut sink, schema_descr, props.clone());
    stream.for_each(|it| {
        for (idx, v) in it.iter().enumerate() {
            match &mut writer.get_column(idx) {
                ColumnWriter::ByteArrayColumnWriter(typed_writer) => {
                    typed_writer.write_batch(&vec![ByteArray::from(*v)], Some(&vec![1i16]), None);
                },
                _ => todo!(),
            }
        }
        futures::future::ready(())
    }).await;

    let meta = writer.close().await?;

    let file_metadata = parquet_format::FileMetaData {
        version: props.writer_version().as_num(),
        schema: parquet::schema::types::to_thrift(schema.as_ref())?,
        num_rows: meta.num_rows() as i64,
        row_groups: vec![meta.to_thrift()],
        key_value_metadata: None,
        created_by: Some(props.created_by().to_owned()),
        column_orders: None,
    };

    let mut metadata_buf = Vec::<u8>::with_capacity(8192);
    let mut protocol = TCompactOutputProtocol::new(&mut metadata_buf);
    file_metadata.write_to_out_protocol(&mut protocol)?;
    protocol.flush()?;

    dbg!(metadata_buf.len());

    let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
    LittleEndian::write_i32(&mut footer_buffer, metadata_buf.len() as i32);
    (&mut footer_buffer[4..]).write(&PARQUET_MAGIC)?;

    io::copy(&mut &metadata_buf[..], &mut sink).unwrap();
    io::copy(&mut &footer_buffer[..], &mut sink).unwrap();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::Compression;
    use parquet::data_type::ByteArray;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use parquet::file::properties::WriterVersion;

    fn create_schema() -> Type {
        use parquet::schema::parser;
        let message = "message schema {
            OPTIONAL BYTE_ARRAY string_field_0 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_1 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_2 (UTF8);
        }";
        parser::parse_message_type(message).expect("Expected valid schema")
    }

    #[test]
    fn test22() {
        use std::io::Cursor;
        let schema = Rc::new(create_schema());
        let props = Rc::new(
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let mut buf = Cursor::new(Vec::<u8>::new());
        futures::executor::block_on(async {
            write_parquet(&mut buf, stream::empty(), schema, props).await.unwrap();
        });
        dbg!(buf.into_inner().len());
    }

    #[test]
    fn test_write_to_file() {
        use futures::io::AllowStdIo;
        let schema = Rc::new(create_schema());
        let props = Rc::new(
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let mut buf = AllowStdIo::new(std::fs::File::create("test.snappy.parquet").unwrap());
        futures::executor::block_on(async {
            write_parquet(&mut buf, stream::iter(vec![
                vec!["asdf1", "news1", "good3"],
                vec!["asdf2", "news2", "good2"],
                vec!["asdf3", "news3", "good1"]
            ]), schema, props).await.unwrap();
        });
    }
}
