import itertools
import pandas as pd

from pyspark import Row

def key_by_cols(*key_cols):
    key_cols = set(key_cols)
    def f(i, p):
        value_cols = None
        for r in p:
            if value_cols is None:
                value_cols = set(r.asDict().keys()) - key_cols
            
            yield (
                Row(**{c:r[c] for c in key_cols}),
                Row(**{c:r[c] for c in value_cols})
            )
    return f

def to_df_chunks(chunk_size=None, index_cols=None, grouped=False):
    def chunker(n, it):
        "grouper(3, 'ABCDEFG') --> ABC DEF G"
        it = iter(it)
        for chunk in iter(lambda: itertools.islice(it, n), []):
            first = chunk.next()
            yield itertools.chain([first], chunk)
    
    def f(i, p):
        if not grouped:
            p = itertools.izip_longest([], [p], fillvalue=None)
        
        for group, rows in p:
            try:
                it = iter(rows)
                first = it.next()
            except StopIteration:
                return
            
            fields = first.__fields__
            data = itertools.chain([first], it)
            
            for chunk in chunker(chunk_size, data):
                if grouped:
                    yield group, pd.DataFrame.from_records(chunk, index=index_cols, columns=fields)
                else:
                    yield pd.DataFrame.from_records(chunk, index=index_cols, columns=fields)
    
    return f
