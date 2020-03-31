use futures::{AsyncSeek, AsyncWrite};
use parquet::{
    column::writer::{get_column_writer, ColumnWriter},
    file::metadata::ColumnChunkMetaData,
    file::properties::WriterProperties,
    schema::types::ColumnDescriptor,
};
use std::{io::Cursor, rc::Rc};
use futures::io::AllowStdIo;
use std::marker::Unpin;

pub struct BufferedColumnWriter {
    buf_ptr: *mut Vec<u8>,
    writer: ColumnWriter,
}

impl BufferedColumnWriter {
    pub fn new(col_desc: ColumnDescriptor, props: Rc<WriterProperties>) -> Self {
        let buf = Box::new(Vec::<u8>::new());
        let buf_ptr = Box::into_raw(buf);
        let cursor = Cursor::new(unsafe { &mut *buf_ptr });
        let page_writer = Box::new(parquet::file::writer::SerializedPageWriter::new(cursor));

        let writer = get_column_writer(Rc::new(col_desc), props, page_writer);

        BufferedColumnWriter { buf_ptr, writer }
    }

    pub fn get_writer(&mut self) -> &mut ColumnWriter {
        &mut self.writer
    }

    pub async fn close<T>(
        self,
        sink: &mut T,
    ) -> Result<(i64, i64, ColumnChunkMetaData), Box<dyn std::error::Error>>
    where
        T: AsyncSeek + AsyncWrite + Unpin + Sized,
    {
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
        let buf = unsafe { Box::from_raw(self.buf_ptr) };
        futures::io::copy(AllowStdIo::new(Cursor::new(buf.as_ref())), sink);
        Ok((bytes_written as i64, rows_written as i64, metadata))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use parquet::basic::Compression;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::schema::types::Type;
    use parquet::schema::types::{ColumnDescriptor, ColumnPath};
    use std::rc::Rc;

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

        let col_desc = ColumnDescriptor::new(
            field.clone(),
            Some(schema.clone()),
            1,
            0,
            ColumnPath::from(field.name()),
        );

        get_column_writer(Rc::new(col_desc), props, writer)
    }

    // fn tmp2(page_writer: Box<SerializedPageWriter<Cursor<&'static mut Vec<u8>>>>) -> ColumnWriter {
    //     let schema = Rc::new(create_schema());
    //     let field = &schema.get_fields()[0];

    //     let props = Rc::new(
    //         WriterProperties::builder()
    //             .set_writer_version(WriterVersion::PARQUET_2_0)
    //             .set_compression(Compression::SNAPPY)
    //             .build(),
    //     );

    //     let col_desc = ColumnDescriptor::new(field.clone(), Some(schema.clone()), 1, 0, ColumnPath::from(field.name()));

    //     get_column_writer(Rc::new(col_desc), props, page_writer)
    // }

    #[test]
    fn test_something() {
        let buf = Box::new(Vec::<u8>::new());
        let buf_ptr = Box::into_raw(buf);
        let writer = tmp(unsafe { &mut *buf_ptr });

        match writer {
            ColumnWriter::ByteArrayColumnWriter(mut typed_writer) => {
                typed_writer.write_batch(
                    &vec![ByteArray::from("asdfnews")],
                    Some(&vec![1i16]),
                    None,
                );
                typed_writer.close();
            }
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
