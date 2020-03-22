use parquet::{
    column::writer::{get_column_writer, ColumnWriter, ColumnWriterImpl},
    column::page::{PageWriter, CompressedPage, PageWriteSpec},
    errors::ParquetError,
    file::metadata::ColumnChunkMetaData,
    file::writer::SerializedPageWriter,
    file::properties::{WriterProperties, WriterVersion},
    schema::types::{ColumnDescriptor, ColumnPath},
};
use std::{io::Cursor, rc::Rc};

struct BufferedColumnWriter {
    buf: *mut Vec<u8>,
    writer: ColumnWriter,
}

impl BufferedColumnWriter {
    pub fn new(col_desc: ColumnDescriptor, props: Rc<WriterProperties>) -> Self {
        let buf = Box::new(Vec::<u8>::new());
        let buf_ptr = Box::into_raw(buf);
        let cursor = Cursor::new(unsafe { &mut *buf_ptr });
        let page_writer = Box::new(parquet::file::writer::SerializedPageWriter::new(cursor));

        let writer = get_column_writer(Rc::new(col_desc), props, page_writer);

        BufferedColumnWriter { buf: buf_ptr, writer }
    }

    pub fn get_writer(&mut self) -> &mut ColumnWriter {
        &mut self.writer
    }

    pub fn close(self) -> Result<(u64, u64, ColumnChunkMetaData, Box<Vec<u8>>), Box<dyn std::error::Error>> {
        let (bytes_written, rows_written, metadata) = match self.writer {
            ColumnWriter::BoolColumnWriter(typed) => typed.close()?,
            ColumnWriter::Int32ColumnWriter(typed) => typed.close()?,
            ColumnWriter::Int64ColumnWriter(typed) => typed.close()?,
            ColumnWriter::Int96ColumnWriter(typed) => typed.close()?,
            ColumnWriter::FloatColumnWriter(typed) => typed.close()?,
            ColumnWriter::DoubleColumnWriter(typed) => typed.close()?,
            ColumnWriter::ByteArrayColumnWriter(typed) => typed.close()?,
            ColumnWriter::FixedLenByteArrayColumnWriter(typed) => typed.close()?,
        };
        Ok((bytes_written, rows_written, metadata, unsafe { Box::from_raw(self.buf) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use parquet::schema::types::Type;
    use std::rc::Rc;
    use parquet::basic::Compression;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::schema::types::{ColumnDescriptor, ColumnPath};
    use parquet::data_type::ByteArray;

    fn tmp(buf: &'static mut Vec<u8>) -> ColumnWriter {
        let cursor = Cursor::new(buf);
        let writer = Box::new(parquet::file::writer::SerializedPageWriter::new(cursor));

        let schema = Rc::new(create_schema());
        let field = &schema.get_fields()[0];

        let props = Rc::new(
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(Compression::SNAPPY)
                .build(),
        );

        let col_desc = ColumnDescriptor::new(field.clone(), Some(schema.clone()), 1, 0, ColumnPath::from(field.name()));

        get_column_writer(Rc::new(col_desc), props, writer)
    }

    fn tmp2(page_writer: Box<SerializedPageWriter<Cursor<&'static mut Vec<u8>>>>) -> ColumnWriter {
        let schema = Rc::new(create_schema());
        let field = &schema.get_fields()[0];

        let props = Rc::new(
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(Compression::SNAPPY)
                .build(),
        );

        let col_desc = ColumnDescriptor::new(field.clone(), Some(schema.clone()), 1, 0, ColumnPath::from(field.name()));

        get_column_writer(Rc::new(col_desc), props, page_writer)
    }

    #[test]
    fn test_something_new() {
        let schema = Rc::new(create_schema());
        let field = &schema.get_fields()[0];
        let props = Rc::new(
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(Compression::SNAPPY)
                .build(),
        );

        let col_desc = ColumnDescriptor::new(field.clone(), Some(schema.clone()), 1, 0, ColumnPath::from(field.name()));

        let mut writer = BufferedColumnWriter::new(col_desc, props);

        match &mut writer.get_writer() {
            // ColumnWriter::ByteArrayColumnWriter(mut typed_writer) => {
            //     typed_writer.write_batch(&vec![ByteArray::from("asdfnews")], Some(&vec![1i16]), None);
            //     typed_writer.close();
            // },
            ColumnWriter::ByteArrayColumnWriter(typed_writer) => {
                eprintln!("Hello World");
                typed_writer.write_batch(&vec![ByteArray::from("asdfnews")], Some(&vec![1i16]), None);
            },
            ColumnWriter::FixedLenByteArrayColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::BoolColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int32ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int64ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int96ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::FloatColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::DoubleColumnWriter(_typed_writer) => todo!(),
        }

        match &mut writer.get_writer() {
            // ColumnWriter::ByteArrayColumnWriter(mut typed_writer) => {
            //     typed_writer.write_batch(&vec![ByteArray::from("asdfnews")], Some(&vec![1i16]), None);
            //     typed_writer.close();
            // },
            ColumnWriter::ByteArrayColumnWriter(typed_writer) => {
                typed_writer.write_batch(&vec![ByteArray::from("blahblah")], Some(&vec![1i16]), None);
            },
            ColumnWriter::FixedLenByteArrayColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::BoolColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int32ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int64ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int96ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::FloatColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::DoubleColumnWriter(_typed_writer) => todo!(),
        }

        dbg!(writer.close().len());

        // dbg!(cursor.position());
        // let dealloc = unsafe { Box::from_raw(buf_ptr) };
        // dbg!(dealloc.len());
    }

    #[test]
    fn test_something() {
        let buf = Box::new(Vec::<u8>::new());
        let buf_ptr = Box::into_raw(buf);
        let writer = tmp(unsafe { &mut *buf_ptr });

        match writer {
            ColumnWriter::ByteArrayColumnWriter(mut typed_writer) => {
                typed_writer.write_batch(&vec![ByteArray::from("asdfnews")], Some(&vec![1i16]), None);
                typed_writer.close();
            },
            ColumnWriter::FixedLenByteArrayColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::BoolColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int32ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int64ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::Int96ColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::FloatColumnWriter(_typed_writer) => todo!(),
            ColumnWriter::DoubleColumnWriter(_typed_writer) => todo!(),
        }

        // dbg!(cursor.position());

        let dealloc = unsafe { Box::from_raw(buf_ptr) };
        dbg!(dealloc.len());
    }

    fn create_schema() -> Type {
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
        parser::parse_message_type(message).expect("Expected valid schema")
    }
}