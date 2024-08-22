import psycopg2
import psycopg2.extras
import random
import datetime
import string
import time
import concurrent.futures


def truncate_table(conn):
    with conn.cursor() as cur:
        cur.execute("truncate todoitems")
        conn.commit()


def human_read_to_byte(size):
    # if no space in between retry
    size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
    i = 0
    while i < len(size):
        if size[i].isnumeric():
            i += 1
        else:
            break
    size = size[:i], size[i:]  # divide '1 GB' into ['1', 'GB']
    num, unit = int(size[0]), size[1]
    idx = size_name.index(
        unit)  # index in list of sizes determines power to raise it to
    factor = 1024 ** idx  # ** is the "exponent" operator - you can use it instead of math.pow()
    return num * factor


def connect_to_postgres(user, password, host, db_name):
    connection = psycopg2.connect(
        user=user,
        password=password,
        dbname=db_name,
        host=host)

    print("Successfully connected to postgres Database")

    return connection


def get_number_of_rows_from_file_size(size):
    # current todoitem schema having 1CR rows amounts to 820M size in rds
    return 10 ** 7 * human_read_to_byte(size) // human_read_to_byte('820M')


def create_table(conn, table_name='todoitems'):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    (   
        id INT,
        description TEXT,
        timestamp TIMESTAMP,
        done INT,
        randomnumber INT,
        randomstring TEXT,
        id2 TEXT
    );

    """
    with conn.cursor() as cursor:
        # Create the Table.
        cursor.execute(create_table_query)

        # Commit the Table.
        conn.commit()
    print('Table is created!')


def process_batch(conn, batch_number, batch_size, number_of_batches,
                  table_name='todoitems', page_size=10000, rows=None):
    t1 = time.time()
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
            row = (
            id, description, timestamp, toggle, task_number2, random_string2,
            task_number3)
            rows.append(row)
            toggle = int(not toggle)
    sql_insert_query = f"INSERT INTO {table_name} (id, description, timestamp, done, randomnumber, randomstring, id2) values %s"
    with conn.cursor() as cursor:
        try:
            psycopg2.extras.execute_values(
                cursor, sql_insert_query, rows, template=None,
                page_size=page_size
            )
            conn.commit()
        except Exception as e:
            print(
                f'{batch_number} // {number_of_batches}: exception got - {str(e).lower()}')
            sleep_time = random.randint(180, 300)
            print(
                f'{batch_number} // {number_of_batches} : sleep - {sleep_time}')
            time.sleep(sleep_time)
            print(
                f'{batch_number} // {number_of_batches} : sleep complete - {sleep_time}')
            process_batch(conn, batch_number, batch_size, number_of_batches,
                          table_name, page_size, rows)
    print(f'{batch_number} // {number_of_batches} : insert done')
    t2 = time.time()
    print(
        str(t2 - t1) + f": Time spent - {batch_number} // {number_of_batches}")


def pump_data(conn, target_size, max_threads=128, batch_size=50000):
    create_table(conn)
    total_rows_required = get_number_of_rows_from_file_size(target_size)
    if total_rows_required < batch_size:
        print('Please reduce batch size to continue')
        return
    number_of_batches = total_rows_required // batch_size
    future_to_batch = {}
    workers = min(max_threads, number_of_batches)
    print('number of workers - {}'.format(workers))
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers) as executor:
        for batch_number in range(1, number_of_batches + 1):
            arg = (conn, batch_number, batch_size, number_of_batches)
            future_to_batch[
                executor.submit(process_batch, *arg)] = batch_number

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
    host = 'sbera-postgres-database-1.cwqrozfgqfcv.us-west-1.rds.amazonaws.com'
    db_name = 'postgres'
    conn = connect_to_postgres(user='cohesity', db_name=db_name, host=host,
                               password='Cohe$1ty')
    pump_data(conn, '800G')