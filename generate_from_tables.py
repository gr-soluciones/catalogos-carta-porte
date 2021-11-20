#!/usr/bin/env python3

from pathlib import Path
from DDLIdempotent.ConvertDDL import convert

TABLES_DIR = Path(__file__).parent / 'tables'

def split_queries_by_type(script_content: str, limit: int = 0) -> tuple:
    create = []
    alter = []
    drop = []
    insert = []

    queries = map(
        lambda s: s.strip(),
        script_content.split(';'))

    for query in queries:
        if not query:
            continue

        q_upper = query.upper()

        if q_upper.startswith('CREATE'):
            create.append(query)
        elif q_upper.startswith('ALTER'):
            alter.append(query)
        elif q_upper.startswith('DROP'):
            drop.append(query)
        elif q_upper.startswith('INSERT'):
            insert.append(query)
        else:
            raise ValueError('Unknown query type: {}'.format(query))
        
        if max([len(l) for l in [create, alter, drop, insert]]) >= limit:
            break
    
    return create, alter, drop, insert


def join_queries(queries: list) -> str:
    return ';\n'.join(queries)


if __name__ == '__main__':

    final_script = []

    for table_file in TABLES_DIR.glob('*.sql'):
        with open(table_file, 'r') as f:
            print('Converting {}'.format(table_file))

            file_data = f.read()
            create, _, _, _ = split_queries_by_type(file_data, 10)

            create = list(map(convert, create))
            final_script.extend(create)
    
    with open('final.sql', 'w') as f:
        f.write(join_queries(final_script))