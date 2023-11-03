#!/usr/bin/env python3
import subprocess
import datetime
import os

def cleaner(line: str) -> str:
    line = line.strip()
    line = line.replace(' ]------------------\nrelation | ', ' ')
    line = line.replace(' MB\n-[', '')
    line = line.replace('\nsize     | ', ' ')
    line = line.replace(' MB', '')
    n, table, size = line.split()
    return (int(n), table, int(size))


DB = 'guvd2013'
PATH = '/mnt/backup/' + str(datetime.date.today()) + '/'
TB_SIZE = 512
THREADS = 12
LOGFILE = PATH + 'LOG.TXT'

# regex = r'^relation \| (?P<table>.+)(?:\s)|^size\s+|\s{1}(?P<size>\d+)\s{1}MB'
command = f'psql -c "\\x" -c "SELECT nspname || \'.\' || relname AS "relation", pg_size_pretty(pg_total_relation_size(C.oid)) AS "size" FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) WHERE nspname NOT IN (\'pg_catalog\', \'information_schema\', \'pg_toast\') AND pg_total_relation_size(C.oid) > {str(TB_SIZE * 1024 ** 2)} ORDER BY pg_total_relation_size(C.oid) DESC;" {DB}'
result = subprocess.run(command, shell=True, capture_output=True)


if result.returncode == 0 and 'RECORD' in result.stdout.decode('utf-8'):
    tables = list(map(cleaner, result.stdout.decode('utf-8').split('RECORD')[1:]))
    excluded_tables = ' '.join([' -T ' + i[1] for i in tables])
    if not os.path.exists(PATH):
        os.makedirs(PATH)

    command = f'pg_dump -U postgres --format=directory --jobs={THREADS} --blobs --encoding UTF8 --verbose {excluded_tables} --file={PATH} {DB}'
    result = subprocess.run(command, shell=True, capture_output=True)

    if result.returncode == 0:
        print('pg_dump success!')
    
    for table in tables:
        print(table)
        command_schema = f'pg_dump -t {table[1]} -s -f {PATH}shema_{table[1]}.sql {DB}'
        command_table_copy = f'psql -U postgres --dbname={DB} --command "\COPY {table[1]} TO \'{PATH}{table[1]}.sql\' WITH BINARY;"'
        result_schema = subprocess.run(command_schema, shell=True, capture_output=True)
        result_table_copy = subprocess.run(command_table_copy, shell=True, capture_output=True)
        if result_schema.returncode == 0 and result_table_copy == 0:
            print(f'Table {table[1]} copied')

