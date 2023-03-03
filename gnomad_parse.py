# convert tsv to SQL db via python

import sqlite3, csv
from pathlib import Path
import pandas as pd
# create empty file

Path('/home/ionadmin/ngs_variant_annotation/variantAnnotation/gnomad_17.sdb').touch()
# SQL conn
with sqlite3.connect('/home/ionadmin/ngs_variant_annotation/variantAnnotation/gnomad_17.sdb', timeout = 100) as conn:
    c = conn.cursor()
'''
Path('gnomad_17.sdb').touch()
with sqlite3.connect('gnomad_17.sdb', timeout = 100) as conn:
    c = conn.cursor()
'''

gnomadfile = 'gnomad.exomes.r2.1.1.sites.17.vcf'

''' Specifying columns is not necessary for tables with column
names
'''

def sql_append_tsv_chunk(chunk):
    # write the data to a sqlite table
    chunk.to_sql('gnomad_17', conn, if_exists='append', index = True)

def AF_extract(info_column):
    # Replace this with your own function
    annotations = info_column.split(';')

    for annotation in annotations:
        key_value_pair = annotation.split('=')
        if key_value_pair[0] == 'AF':
            af_value = key_value_pair[1]
            return af_value

def parse_row(line):
    column_index = 7
    # Process each row of the table
    # Access the appropriate column using its index in the tuple
    af_value = AF_extract(line[column_index])
    # Update the appropriate value
    line[column_index] = af_value
    # Convert the row back to a tuple and return it
    return(line)

with open(gnomadfile,'r') as gnomread:
    for line in gnomread:
        if not line.strip().startswith("#"):
            line = gnomread.readline()
            line = line.split('\t')
            if len(line) == 8:
                line = parse_row(line)
                line = [line]
                df = pd.DataFrame(line, columns=['CHROM', 'POS', 'ID','REF','ALT','QUAL', 'FILTER','AF'])
                sql_append_tsv_chunk(df)

c.close()
