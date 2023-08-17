import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process songs files and insert records into the Postgres database.
    :param cur: cursor reference 这是 PostgreSQL 数据库的游标引用，允许函数执行 SQL 命令。
    :param filepath: complete file path for the file to load 这是需要处理的歌曲文件的路径。
    """
    # 这个名为 "process_song_file "的函数用于处理单个歌曲文件，并将相关数据插入 PostgreSQL 数据库。下面是该函数的具体操作步骤：
    
    # 此行将 JSON 格式的歌曲文件读入 pandas DataFrame。文件以系列（`typ='series'`）的形式读取，因为它预计只包含一条记录。然后，该系列被包裹在一个列表中，以将其转换为 DataFrame。
    # open song file
    df = pd.DataFrame([pd.read_json(filepath, typ='series', convert_dates=False)])
    
    # 循环遍历 DataFrame 中的记录。由于 DataFrame 预计只包含一行（来自一个歌曲文件），因此该循环将运行一次。
    for value in df.values:
        # "开头的行从 DataFrame 行中提取单个数据点，并将它们赋值给相应的变量。
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

        # insert artist record
        # 将艺术家数据结构化为元组，然后使用 `artist_table_insert` SQL 命令将其插入数据库。
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artist_data)

        # insert song record
        # 同样，歌曲数据被结构化为一个元组，然后使用`song_table_insert` SQL 命令插入数据库。
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)
    
    print(f"Records inserted for file {filepath}")


def process_log_file(cur, filepath):
    """
    Process Event log files and insert records into the Postgres database.
    :param cur: cursor reference
    :param filepath: complete file path for the file to load
    """
    # open log file
    df = df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"].astype({'ts': 'datetime64[ms]'})

    # convert timestamp column to datetime
    t = pd.Series(df['ts'], index=df.index)
    
    # insert time data records
    column_labels = ["timestamp", "hour", "day", "weelofyear", "month", "year", "weekday"]
    time_data = []
    for data in t:
        time_data.append([data ,data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])

    time_df = pd.DataFrame.from_records(data = time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = ( row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Driver function to load data from songs and event log files into Postgres database.
    :param cur: a database cursor reference 指向 PostgreSQL 数据库的游标引用，允许函数执行 SQL 命令。
    :param conn: database connection reference 数据库连接引用，用于向数据库提交事务。
    :param filepath: parent directory where the files exists JSON 文件所在的目录路径。
    :param func: function to call 处理每个文件时应调用的函数的引用。根据上下文，可以是 `process_song_file` 或 `process_log_file`。
    """
    # process_data "函数是一个驱动函数，用于处理 JSON 文件（歌曲文件或事件日志文件）中的数据并将其加载到 PostgreSQL 数据库中。下面是其功能的细分：
    # get all files matching extension from directory
    # 该函数首先初始化一个空列表 `all_files`。然后，它从 `filepath` 开始遍历所有目录和子目录，并收集找到的所有 JSON 文件。这些文件路径会追加到 `all_files` 列表中。
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    #  该函数计算并打印在指定目录中找到的 JSON 文件总数。
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # 然后，函数遍历`all_files`列表中的每个文件。对于每个文件：
    # 它调用指定的 `func`（`process_song_file` 或 `process_log_file`）来处理文件。
    # 使用 `conn.commit()` 向数据库提交事务。
    # 打印一条进度消息，说明总共处理了多少文件。
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Driver function for loading songs and log data into Postgres database
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user= password=")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
    print("\n\nFinished processing!!!\n\n")