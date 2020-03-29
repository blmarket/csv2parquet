use crate::wrong_column_writer::BufferedColumnWriter;
use futures::io::{AsyncWriteExt};
use futures_io::AsyncWrite;
use parquet::{
    column::writer::ColumnWriter,
    file::{
        metadata::{ColumnChunkMetaData, RowGroupMetaData},
        properties::{WriterProperties},
    },
    schema::types::{SchemaDescPtr, SchemaDescriptor},
};
use std::rc::Rc;

pub struct BufferedRowGroupWriter<W: AsyncWrite + Unpin> {
    sink: W,
    columns: Vec<BufferedColumnWriter>,
    schema_descr: SchemaDescPtr,
}

impl<W: AsyncWrite + Unpin> BufferedRowGroupWriter<W> {
    pub fn new(sink: W, schema: Rc<SchemaDescriptor>, writer_props: Rc<WriterProperties>) -> Self {
        use parquet::schema::types::{ColumnDescriptor, ColumnPath};

        let fields = schema.root_schema().get_fields();
        let columns = fields.iter().map(|field| {
            let col_desc = ColumnDescriptor::new(field.clone(), None, 1, 0, ColumnPath::from(field.name()));
            BufferedColumnWriter::new(col_desc, writer_props.clone())
        }).collect();

        BufferedRowGroupWriter { sink, columns, schema_descr: schema }
    }

    pub fn get_column(&mut self, idx: usize) -> &mut ColumnWriter {
        self.columns[idx].get_writer()
    }

    pub async fn close(mut self) -> Result<Rc<RowGroupMetaData>, Box<dyn std::error::Error>> {
        let mut total_bytes_written = 0i64;
        let mut total_rows: Option<i64> = None;
        let mut metas: Vec<ColumnChunkMetaData> = Vec::with_capacity(self.columns.len());
        for col in self.columns {
            let (bytes_written, rows_written, metadata, bytes_to_write) = col.close().unwrap();
            total_bytes_written += bytes_written;
            total_rows = match (total_rows, rows_written) {
                (None, y) => Some(y),
                (Some(x), y) => {
                    if x != y {
                        panic!("Inconsistent row counts");
                    }
                    Some(x)
                }
            };
            self.sink.write_all(&bytes_to_write).await?;
            metas.push(metadata);
        }
        let row_group_meta = RowGroupMetaData::builder(self.schema_descr)
            .set_column_metadata(metas)
            .set_total_byte_size(total_bytes_written)
            .set_num_rows(total_rows.unwrap_or(0))
            .build()?;
        Ok(Rc::new(row_group_meta))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::schema::types::Type;
    use parquet::basic::Compression;
    use parquet::data_type::ByteArray;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use parquet::file::properties::WriterVersion;
    use futures::io::Cursor;

    fn create_schema() -> Type {
        use parquet::schema::parser;
        let message = "message schema {
            OPTIONAL BYTE_ARRAY snapshot_day (UTF8);
            OPTIONAL BYTE_ARRAY vendor_code (UTF8);
            OPTIONAL BYTE_ARRAY merchant_sku (UTF8);
        }";
        parser::parse_message_type(message).expect("Expected valid schema")
    }

    #[test]
    fn test1() {
        let schema = Rc::new(SchemaDescriptor::new(Rc::new(create_schema())));
        let mut cursor = Cursor::new(Vec::new());
        let props = Rc::new(
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let mut writer = BufferedRowGroupWriter::new(&mut cursor, schema, props);

        for i in 0..10 {
            for j in 0..3 {
                match &mut writer.get_column(j) {
                    ColumnWriter::ByteArrayColumnWriter(typed_writer) => {
                        typed_writer.write_batch(&vec![ByteArray::from("asdfnews")], Some(&vec![1i16]), None);
                    },
                    _ => todo!(),
                }
            }
        }
        
        futures::executor::block_on(async {
            writer.close().await.unwrap();
        });
        eprintln!("{}", cursor.position());
    }
}