Csv2parquet
-----------

Converting csv 2 parquet as fast as I can, using rust.

### Why?

Being forced to waste a lot of time just waiting my slow machine reading bunch
of CSV files, I was just curious how much I can improve on this end.

### Why Rust?

Modern C++ is getting harder to catch-up. My C/C++ code style is more like 
just C + STL libraries, not leveraging recent C++ standards. It was easier to 
learn Rust from the scratch, instead of improving my poor C style I'm familiar 
with more than 10 years.

### Implementation details

As current rust parquet implementation does not have buffered column writer
like [cpp version][1], I had to implement my own version of buffered column
writer, and some copy-and-paste version of row group writer etc.

[1]: https://github.com/apache/arrow/blob/master/cpp/examples/parquet/parquet-stream-api/stream-reader-writer.cc

### Future improvements

Currently buffer is just using `vec[u8]` where a lot of copy will happen if
we write more and more data. Would be nice if I can find some linked list
buffer supporting append operation.
