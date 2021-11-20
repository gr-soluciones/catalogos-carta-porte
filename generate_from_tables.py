#!/usr/bin/env python3

from pathlib import Path

TABLES_DIR = Path(__file__).parent / 'tables'

if __name__ == '__main__':
    for table_file in TABLES_DIR.glob('*.sql'):
        with open(table_file, 'r') as f:
            file_data = f.read()