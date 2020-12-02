use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use parquet::schema::types::Type;

use csv2parquet::writer::write_parquet;

fn create_schema() -> Type {
    use parquet::schema::parser;
    let message = "message schema {
            OPTIONAL BYTE_ARRAY string_field_0 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_1 (UTF8);
            OPTIONAL BYTE_ARRAY string_field_2 (UTF8);
        }";
    parser::parse_message_type(message).expect("Expected valid schema")
}

fn main() {
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