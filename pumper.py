import random
import concurrent.futures
import ceODBC
import sqlserverport
import datetime
import time
import string
import sys
def human_read_to_byte(size):
    # if no space in between retry
    size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
    i = 0
    while i < len(size):
        if size[i].isnumeric():
            i += 1
        else:
            break
    size = size[:i], size[i:]              # divide '1 GB' into ['1', 'GB']
    num, unit = int(size[0]), size[1]
    idx = size_name.index(unit)        # index in list of sizes determines power to raise it to
    factor = 1024 ** idx               # ** is the "exponent" operator - you can use it instead of math.pow()
    return num * factor

def get_number_of_rows_from_file_size(size):
    # current todoitem schema having 11638091 rows amounts to 1G size
    return 5505753 * human_read_to_byte(size) // human_read_to_byte('1G')

def get_log_size(conn, database_name):
    query = f"""
    SELECT
    name,
    size,
    size * 8/1024 'Size (MB)',
    max_size
    FROM sys.master_files where name='{database_name}_log';
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
    size = result[2]
    return size

def create_table(conn, database, table_name='todoitems'):
    create_table_query = f"""
    -- Create the Table if it Does not exist.
    IF Object_ID('{table_name}') IS NULL

    CREATE TABLE [{database}].[dbo].[{table_name}]
    (   
        [id] INT NULL,
        [description] NVARCHAR(MAX) NULL,
        [timestamp] DATETIME NULL,
        [done] INT NULL,
        [randomnumber] INT NULL,
        [randomstring] NVARCHAR(MAX) NULL,
        [id2] NVARCHAR(MAX) NOT NULL,
    )
    """
    with conn.cursor() as cursor:
        # Create the Table.
        cursor.execute(create_table_query)

        # Commit the Table.
        conn.commit()


def process_batch(conn, database, batch_number, batch_size, number_of_batches, table_name, rows=None):
    if not rows:
        rows = []
    else:
        print(f'{batch_number} // {number_of_batches} : retrying')
    toggle = 0
    ascii_letters = list(string.ascii_letters)
    print(f'{batch_number} // {number_of_batches} : preparing')
    sql_server_max_int = 2147483647
    if len(rows) < 1:
        for i in range(batch_size):
            id = random.randint(1, sql_server_max_int)
            description = ''.join(random.choices(ascii_letters, k=10))
            timestamp = datetime.datetime.now()
            task_number2 = random.randint(1, sql_server_max_int)
            random_string2 = ''.join(random.choices(ascii_letters, k=10))
            task_number3 = random.randint(1, sql_server_max_int)
            row = (id,description,timestamp, toggle, task_number2, random_string2,task_number3)
            rows.append(row)
            toggle = int(not toggle)

    sql_insert = f"""
    INSERT INTO [{database}].[dbo].[{table_name}]
    (
        [id],
        [description],
        [timestamp],
        [done],
        [randomnumber],
        [randomstring],
        [id2]
    )
    VALUES
    (
        ?, ?, ?, ?, ?, ?, ?
    )
    """
    with conn.cursor() as cursor:
        print(f'{batch_number} // {number_of_batches} : inserting')
        try:
            cursor.executemany(sql_insert, rows)
            conn.commit()
        except Exception as e:
            if 'connection is busy' in str(e).lower():
                sleep_time = random.randint(180, 300)
                print(f'{batch_number} // {number_of_batches} : sleep - {sleep_time}')
                time.sleep(sleep_time)
                print(f'{batch_number} // {number_of_batches} : sleep complete - {sleep_time}')
                process_batch(conn, database, batch_number, batch_size, number_of_batches, table_name, rows)
    print(f'{batch_number} // {number_of_batches} : insert done')


def pump_data(conn, database_name, target_size, max_threads=128, batch_size=50000, table_name='todoitems'):
    create_table(conn, database_name)
    total_rows_required = get_number_of_rows_from_file_size(target_size)
    if total_rows_required < batch_size:
        print('Please reduce batch size to continue')
        return
    number_of_batches = total_rows_required // batch_size
    future_to_batch = {}
    workers = min(max_threads, number_of_batches)
    print('number of workers - {}'.format(workers))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for batch_number in range(1, number_of_batches + 1):
            arg = (conn, database_name, batch_number, batch_size, number_of_batches, table_name)
            future_to_batch[executor.submit(process_batch, *arg)] = batch_number

    result = []
    for future in concurrent.futures.as_completed(future_to_batch):
        batch_number = future_to_batch[future]
        try:
            res = future.result()
            if not res:
                result.append(batch_number)
        except Exception as exc:
            print("%r generated an exception: %s" % (batch_number, exc))
            # todo: handle here sequentially for error batches

if __name__ == '__main__':
    SERVER = '10.14.69.135'
    DATABASE = 'single7'
    USERNAME = 'sa'
    PASSWORD = 'cohesity'
    serverspec = '{0},{1}'.format(
        SERVER,
        sqlserverport.lookup(SERVER, 'SYSTEST'))
    connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER}\SYSTEST;DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=no'

    # Create a connection object.
    conn = ceODBC.connect(connectionString)
    pump_data(conn, DATABASE, '5G')
