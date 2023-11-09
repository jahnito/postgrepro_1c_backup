#!/usr/bin/env python3
import subprocess
import datetime
import os
import logging


logging.basicConfig(level=logging.INFO, filename='backups.log', filemode='a', format="%(asctime)s %(levelname)s %(message)s")

class DataBaseBackup():
    def __init__(self, db, path, reg, depth, threads=4):
        self.db = db
        self.path = path
        self.reg = reg
        self.depth = depth
        self.excluded_tables = self.find_big_tables(db)
        self.threads = threads
        self.bk_folder = str(datetime.date.today())
        self.day = int(datetime.date.today().strftime('%j'))
        logging.info(f'################## Приступаем к обработке базы {self.db} ##################')

    def run(self):
        # Проверяем необходимость создания лога в соответствии с заданной регулярностью
        if self.day % self.reg == 0:
            backups = []
            p = self.path + self.db
            np = p + '/' + self.bk_folder
            if os.path.exists(p):
                folders = os.listdir(p)
                for f in folders:
                    if os.path.isdir(p + '/' +f) and len(f.split('-')) == 3:
                        backups.append(f)
            else:
                os.makedirs(p)
            backups.sort(key=lambda x: x.split())

            if len(backups) >= self.depth:
                # Удаляем папку с самой старой резервной копией
                logging.info(f'Удаляется устаревший каталог с копией {p + "/" + backups[0]} | {self.db}')
                for fl in os.listdir(p + '/' + backups[0]):
                    os.remove(p + '/' + backups[0] + '/' + fl)
                os.rmdir(p + '/' + backups[0])

            # Создаем папку для новой копии
            os.makedirs(np)

            # Если есть переполненные таблицы, то идём по этой ветке
            if self.excluded_tables:
                logging.info(f'Обнаружены таблицы с переполнением в 512 Мб | {self.db}')
                for t in self.excluded_tables:
                    size = 'Gb' if t[2] < 512 else 'Mb'
                    logging.info(f'Таблица {t[1]} размером {t[2]} {size} | {self.db}')
                logging.info(f'Исполняется PG_DUMP с использованием {self.threads} потоков | {self.db}')
                excluded = ' '.join([' -T ' + i[1] for i in self.excluded_tables])
                command = f'pg_dump -U postgres --format=directory --jobs={self.threads} --blobs --encoding UTF8 --verbose {excluded} --file={np} {self.db}'
                result = subprocess.run(command, shell=True, capture_output=True)
                if result.returncode == 0:
                    logging.info(f'PG_DUMP успешно завершил работу | {self.db}')
                else:
                    logging.error(f'PG_DUMP завершил работу с ошибками | {self.db}')

                for table in self.excluded_tables:
                    command_schema = f'pg_dump -t {table[1]} -s -f {np}/shema_{table[1]}.sql {self.db}'
                    command_table_copy = f'psql -U postgres --dbname={self.db} --command "\COPY {table[1]} TO \'{np}/{table[1]}.sql\' WITH BINARY;"'
                    result_schema = subprocess.run(command_schema, shell=True, capture_output=True)
                    if result_schema.returncode == 0:
                        logging.info(f'Копирование схемы таблицы {table[1]} завершено успешно | {self.db}')
                    else:
                        logging.error(f'ВНИМАНИЕ. Копирование схемы {table[1]} не завершено! Произошла ошибка! | {self.db}')

                    result_table_copy = subprocess.run(command_table_copy, shell=True, timeout=1800, capture_output=True)
                    if result_table_copy.returncode == 0:
                        logging.info(f'Бинарное копирование таблицы {table[1]} завершено успешно | {self.db}')
                    else:
                        logging.error(f'ВНИМАНИЕ. Бинарное копирование таблицы {table[1]} не завершено! Произошла ошибка! | {self.db}')
            else:
                # Если нет переполненных таблиц производим стандартный дамп
                command = f'pg_dump -U postgres --format=directory --jobs={self.threads} --blobs --encoding UTF8 --verbose --file={np} {self.db}'
                result = subprocess.run(command, shell=True, capture_output=True)
                if result.returncode == 0:
                    logging.info(f'PG_DUMP успешно завершил работу | {self.db}')
                else:
                    logging.error(f'PG_DUMP завершил работу с ошибками | {self.db}')
        else:
            logging.info(f'База данных {self.db} не подлежит резервному копированию по графику')
        logging.info(f'################## Завершаем обработку базы {self.db} ##################')

    def cleaner(self, line: str) -> str:
        '''
        Метод очищает вывод и формирует кортеж с данными:
        n - порядковый номер таблицы
        table - имя таблицы
        size - размер таблицы в Mb или Gb
        '''
        line = line.strip()
        line = line.replace('-', '')
        line = line.replace(' ]\nrelation | ', ' ')
        line = line.replace(' GB\n[', '')
        line = line.replace(' MB\n[', '')
        line = line.replace('\nsize     | ', ' ')
        line = line.replace(' GB', '')
        line = line.replace(' MB', '')
        n, table, size = line.split()
        return (int(n), table, int(size))

    def find_big_tables(self, db):
        '''
        Функция находит таблицы превышающие размер 512 Мб
        для дальнейшего резервного копирования их в бинарном режиме
        '''
        failure_size = 512 * 1024 ** 2
        show_big_tables = f'psql -c "\\x" -c "SELECT nspname || \'.\' || relname AS "relation", pg_size_pretty(pg_total_relation_size(C.oid)) AS "size" FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) WHERE nspname NOT IN (\'pg_catalog\', \'information_schema\', \'pg_toast\') AND pg_total_relation_size(C.oid) > {str(failure_size)} ORDER BY pg_total_relation_size(C.oid) DESC;" {db}'
        result = subprocess.run(show_big_tables, shell=True, capture_output=True)
        if result.returncode == 0:
            if 'RECORD' in result.stdout.decode('utf-8').split('\n')[1]:
                return list(map(self.cleaner, result.stdout.decode('utf-8').split('RECORD')[1:]))
            
        return []


# Базы данных требующие регулярное резервирование
# Ключ словаря DBS наименование базы данных
# DEPTH - количество сохраняемых копий
# REGULARITY - регулярность выполнения, через каждые N дней
DBS = {
       '1cguvd_2': {'DEPTH': 14, 'REGULARITY': 1},

       '1cguvd_2022': {'DEPTH': 4, 'REGULARITY': 15},
       'postgres': {'DEPTH': 1, 'REGULARITY': 15},
       'template0': {'DEPTH': 1, 'REGULARITY': 15},
       'template1': {'DEPTH': 1, 'REGULARITY': 15},
       'oad': {'DEPTH': 1, 'REGULARITY': 15},
       'momotova': {'DEPTH': 1, 'REGULARITY': 15},

       'guvd2013': {'DEPTH': 1, 'REGULARITY': 1024},
       'guvd2014': {'DEPTH': 1, 'REGULARITY': 1024},
       'guvd2015': {'DEPTH': 1, 'REGULARITY': 1024},
       '1cguvd_2016': {'DEPTH': 1, 'REGULARITY': 1024},
       '1cguvd_2017': {'DEPTH': 1, 'REGULARITY': 1024},
       '1cguvd_2018': {'DEPTH': 1, 'REGULARITY': 1024},
       '1cguvd_2019': {'DEPTH': 1, 'REGULARITY': 1024},
       '1cguvd_2020': {'DEPTH': 1, 'REGULARITY': 1024},
       '1cguvd_2021': {'DEPTH': 1, 'REGULARITY': 1024},
       }


# Период создания резервных копий баз данных
REGULARITY = 30
# Глубина хранения копий баз данных
DEPTH = 2
# Путь для хранения резерной копии
PATH = '/mnt/backup/'


if __name__ == '__main__':
    for db in DBS:
        bk = DataBaseBackup(db,
                            DBS[db].get('PATH', PATH),
                            DBS[db].get('REGULARITY', REGULARITY),
                            DBS[db].get('DEPTH', DEPTH),
                            threads=24)
        bk.run()
