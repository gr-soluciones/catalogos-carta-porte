#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
from re import compile

DATA_DIR = Path('../CatalogosCartaPorte20')
FILE_TYPE = '.csv'
RE_TRILING_ZEROS = compile(r'^0+[^0]+')
RE_DATE = compile(r'^\d{1,2}/\d{1,2}/\d{1,2}$')

if __name__ == '__main__':
    # Load all CSVs in the directory
    files = [f for f in DATA_DIR.glob('**/*' + FILE_TYPE)]

    files = dict(map(lambda x: (x.name[:x.name.index('-')], str(x)), files))

    files_dyn_fmt = {
        nm: pd.read_csv(ph) for nm, ph in files.items()
    }
    files_str = {
        nm: pd.read_csv(ph, dtype=str) for nm, ph in files.items()
    }
    date_cols = {}

    for k in files_str.keys():
        print('\nProcessing table:', k)
        # Columnas que cambiaron de formato
        # col, type1, type2
        conv = pd.concat([files_str[k].dtypes, files_dyn_fmt[k].dtypes], axis=1)
        conv = conv[conv[0] != conv[1]]

        bad_rows = []

        for row in conv.itertuples():
            sub = files_str[k][row.Index]
            sub = sub[~sub.isna()]
            if sub.empty:
                continue

            fmt_error = RE_TRILING_ZEROS.match(sub.min())
            if not fmt_error:
                continue
            print('\tParsing error for:', row.Index)
            bad_rows.append(row.Index)
        
        files_dyn_fmt[k] = pd.read_csv(files[k], dtype={col: str for col in bad_rows})
        print('\tFixed', len(bad_rows), 'rows')
        files_dyn_fmt[k] = files_dyn_fmt[k].dropna(axis=0, how='all')
        c_names = {c: (c.upper().strip()
            .replace(' ', '_')
            .replace('_DE_', '_')
            .replace('_Y_', '_')
            .replace('Á', 'A')
            .replace('É', 'E')
            .replace('Í', 'I')
            .replace('Ó', 'O')
            .replace('Ú', 'U')
            .replace('/', '_')
            .replace('.', '')
            .replace('\n', '_')
            .replace('_O_', '_')
            .replace('__', '_')) for c in files_dyn_fmt[k].columns}
        files_dyn_fmt[k] = files_dyn_fmt[k].rename(columns=c_names)
        files_str[k] = files_str[k].rename(columns=c_names)

        def fmt_date(date: str) -> str:
            if isinstance(date, float) or not date or not RE_DATE.match(date):
                return date
            date = date.strip().split('/')
            return f'''20{date[2].strip()}-{date[0].strip().rjust(2, '0')}-{date[1].strip().rjust(2, '0')}'''
        
        for col in files_str[k].columns:
            # Check if is date
            sample = files_str[k][col]
            sample = sample[~sample.isna()]
            sample = sample.sample(n=min(len(sample), 30))
            sample = sample.map(lambda x: bool(RE_DATE.match(x)))
            if sample.sum() == sample.shape[0]:
                files_dyn_fmt[k][col] = files_str[k][col].map(fmt_date)
                print('\tDate column:', col)
                date_cols[k] = date_cols.get(k, []) + [col]


    print('\n\nGenerating script . . .\n\n')

    def interpret_types(df, col, file_name):
        dtype = df[col].dtype
        if col in date_cols.get(file_name, set()):
            return f'DATE'
        elif dtype == int:  
            return 'INT'
        elif dtype == float:
            return 'FLOAT'
        elif dtype == 'object':
            return f'VARCHAR({df[col].map(lambda e: len(str(e))).max() + 1})'
        
        raise ValueError(f'Unknown type: {dtype}')

    def custom_tb_definition(df, tb_name: str, file_name) -> str:
        return f"""CREATE TABLE {tb_name} (""" + \
        ',\n    '.join([f'{col} {interpret_types(df, col, file_name)}' for col in df.columns])
    
    def insert_script(df, tb_name):
        sql_texts = []
        for _, row in df.iterrows():
            values = str(tuple(row.values)).replace('nan', 'NULL')
            sql_texts.append('INSERT INTO '+tb_name+' ('+ str(', '.join(df.columns))+ ') VALUES '+ values + ';')
        return '\n'.join(sql_texts)

    with open('carta_porte.sql', 'w') as f, open('carta_porte_insert.sql', 'w') as f2:
        for k in files_dyn_fmt.keys():
            tb_name = k.upper().strip().replace('C_', 'CPORTE_')
            definition = custom_tb_definition(files_dyn_fmt[k], tb_name, k)
            print('\n\n' + definition)
            f.write(definition + ');\n\n')

            f2.write(f'\n\n-- Inserting into {tb_name} ...\n')
            f2.write(insert_script(files_dyn_fmt[k], tb_name))

        