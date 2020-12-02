import pandas as pd
import difflib
from tabulate import tabulate
import sys

pdf = pd.read_parquet("test.snappy.parquet")

diffs = list(difflib.context_diff(tabulate(pdf), """-  -----  -----  -----
0  asdf1  news1  good3
1  asdf2  news2  good2
2  asdf3  news3  good1
-  -----  -----  -----"""))

for d in diffs:
    print(d)

assert diffs == []
