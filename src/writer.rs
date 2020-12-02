// use crate::row_group::BufferedRowGroupWriter;
use parquet::{
    column::buf_writer::ColumnWriter,
    file::{
        properties::{WriterProperties},
    },
    schema::types::{SchemaDescriptor, Type},
    data_type::ByteArray,
};
use std::error::Error;
use std::rc::Rc;
use parquet_format;
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use byteorder::{ByteOrder, LittleEndian};
use std::io::{self, Write, Seek};
use std::iter::Iterator;

use crate::row_group::BufferedRowGroupWriter;

const FOOTER_SIZE: usize = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];
// const ROW_GROUP_SIZE: usize = 1 << 20;

pub fn write_parquet<'a, S, W>(
    mut sink: &mut W,
    stream: S,
    schema: Type,
    props: WriterProperties,
) -> Result<(), Box<dyn Error>>
    where
        S: Iterator<Item=&'a Vec<String>>,
        W: Write + Seek,
{
    io::copy(&mut &PARQUET_MAGIC[..], &mut sink).unwrap();

    let schema_rc = Rc::new(schema);
    let props_rc = Rc::new(props);

    let schema_descr = Rc::new(SchemaDescriptor::new(schema_rc.clone()));

    let mut writer = BufferedRowGroupWriter::new(&mut sink, schema_descr, props_rc.clone());
    stream.for_each(|it| {
        for (idx, v) in it.iter().enumerate() {
            match &mut writer.get_column(idx) {
                ColumnWriter::ByteArrayColumnWriter(typed_writer) => {
                    typed_writer.write_batch(&vec![ByteArray::from(v.as_str())], Some(&vec![1i16]), None).unwrap();
                }
                _ => todo!(),
            }
        }
    });

    let meta = writer.close().unwrap();

    let file_metadata = parquet_format::FileMetaData {
        version: props_rc.writer_version().as_num(),
        schema: parquet::schema::types::to_thrift(schema_rc.as_ref())?,
        num_rows: meta.num_rows() as i64,
        row_groups: vec![meta.to_thrift()],
        key_value_metadata: None,
        created_by: Some(props_rc.created_by().to_owned()),
        column_orders: None,
    };

    let mut metadata_buf = Vec::<u8>::with_capacity(8192);
    let mut protocol = TCompactOutputProtocol::new(&mut metadata_buf);
    file_metadata.write_to_out_protocol(&mut protocol)?;
    protocol.flush()?;

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
    use parquet::file::properties::WriterVersion;
    use std::iter;

    fn create_schema() -> Type {
        use parquet::schema::parser;
        let message = "message schema {
            OPTIONAL BYTE_ARRAY string_field_0 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_1 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_2 (UTF8);
        }";
        parser::parse_message_type(message).expect("Expected valid schema")
    }

    #[allow(dead_code)]
    fn create_schema_10() -> Type {
        use parquet::schema::parser;
        let message = "message schema {
            OPTIONAL BYTE_ARRAY string_field_0 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_1 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_2 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_3 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_4 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_5 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_6 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_7 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_8 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_9 (UTF8);
        }";
        parser::parse_message_type(message).expect("Expected valid schema")
    }

    #[test]
    fn test22() {
        use std::io::Cursor;
        let schema = create_schema();
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::SNAPPY)
            .build();
        let mut buf = Cursor::new(Vec::<u8>::new());
        write_parquet(&mut buf, iter::empty(), schema, props).unwrap();
        dbg!(buf.into_inner().len());
    }

    #[test]
    fn test_write_to_file() {
        use futures::io::AllowStdIo;
        let schema = create_schema();
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::SNAPPY)
            .build();
        let mut buf = AllowStdIo::new(std::fs::File::create("test.snappy.parquet").unwrap());
        write_parquet(&mut buf, [
            vec![String::from("asdf1"), String::from("news1"), String::from("good3")],
            vec![String::from("asdf2"), String::from("news2"), String::from("good2")],
            vec![String::from("asdf3"), String::from("news3"), String::from("good1")]
        ].iter(), schema, props).unwrap();
    }
}
