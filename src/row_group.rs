use std::{rc::Rc, io::{Write, Seek}};

use parquet::{
    column::buf_writer::{self, ColumnWriter},
    file::{
        metadata::{ColumnChunkMetaData, RowGroupMetaData},
        properties::{WriterProperties},
    },
    schema::types::{SchemaDescPtr, SchemaDescriptor},
};

pub struct BufferedRowGroupWriter<W: Sized + Write + Seek> {
    sink: W,
    columns: Vec<ColumnWriter>,
    schema_descr: SchemaDescPtr,
}

impl<W: Write + Seek> BufferedRowGroupWriter<W> {
    pub fn new(sink: W, schema: Rc<SchemaDescriptor>, writer_props: Rc<WriterProperties>) -> Self {
        use parquet::schema::types::{ColumnDescriptor, ColumnPath};

        let fields = schema.root_schema().get_fields();
        let columns = fields.iter().map(|field| {
            let col_desc = ColumnDescriptor::new(field.clone(), None, 1, 0, ColumnPath::from(field.name()));
            buf_writer::get_column_writer(Rc::new(col_desc), writer_props.clone())
        }).collect();

        BufferedRowGroupWriter { sink, columns, schema_descr: schema }
    }

    pub fn get_column(&mut self, idx: usize) -> &mut ColumnWriter {
        &mut self.columns[idx]
    }

    pub fn close(mut self) -> Result<Rc<RowGroupMetaData>, Box<dyn std::error::Error>> {
        let mut total_bytes_written = 0u64;
        let mut total_rows: Option<u64> = None;
        let mut metas: Vec<ColumnChunkMetaData> = Vec::with_capacity(self.columns.len());
        for col in self.columns {
            let (bytes_written, rows_written, metadata) = match col {
                ColumnWriter::BoolColumnWriter(typed) => typed.close(&mut self.sink).unwrap(),
                ColumnWriter::Int32ColumnWriter(typed) => typed.close(&mut self.sink).unwrap(),
                ColumnWriter::Int64ColumnWriter(typed) => typed.close(&mut self.sink).unwrap(),
                ColumnWriter::Int96ColumnWriter(typed) => typed.close(&mut self.sink).unwrap(),
                ColumnWriter::FloatColumnWriter(typed) => typed.close(&mut self.sink).unwrap(),
                ColumnWriter::DoubleColumnWriter(typed) => typed.close(&mut self.sink).unwrap(),
                ColumnWriter::ByteArrayColumnWriter(typed) => typed.close(&mut self.sink).unwrap(),
                ColumnWriter::FixedLenByteArrayColumnWriter(typed) => typed.close(&mut self.sink).unwrap(),
            };
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
            metas.push(metadata);
        }
        let row_group_meta = RowGroupMetaData::builder(self.schema_descr)
            .set_column_metadata(metas)
            .set_total_byte_size(total_bytes_written as i64)
            .set_num_rows(total_rows.unwrap_or(0u64) as i64)
            .build()?;
        Ok(Rc::new(row_group_meta))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_schema() {
        use parquet::schema::parser;
        let message = "message schema {
            OPTIONAL BYTE_ARRAY snapshot_day (UTF8);
            OPTIONAL BYTE_ARRAY vendor_code (UTF8);
            OPTIONAL BYTE_ARRAY merchant_sku (UTF8);
        }";
        parser::parse_message_type(message).expect("Expected valid schema");
    }
}