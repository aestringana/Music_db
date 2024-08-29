# ╔═══════════════════════════════════════════════════════════════════╗
# ║                                                                   ║
# ║                       Code by Andres Estringana                   ║
# ║                           Version 1.3                             ║
# ║    (Update: Now reads and saves CSV files to a database)          ║
# ║                                                                   ║
# ╚═══════════════════════════════════════════════════════════════════╝

import psycopg2
import csv

def create_database_connection():
    connection = psycopg2.connect(
        dbname='music_db',
        user='postgres',
        password='1234',
        host='localhost'
    )
    return connection

def read_csv_data(file_path):
    csv_data = []
    with open(file_path, 'r', encoding='ISO-8859-1') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row if present
        for row in csv_reader:
            csv_data.append(row)
    return csv_data

def check_duplicate_song(cursor, title, artist):
    query = "SELECT COUNT(*) FROM songs WHERE title = %s AND artist = %s"
    cursor.execute(query, (title, artist))
    count = cursor.fetchone()[0]
    return count > 0

def insert_data_into_database(connection, data):
    cursor = connection.cursor()  # Initialize cursor here

    # Create the 'songs' table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS songs (
            id SERIAL PRIMARY KEY,
            title VARCHAR(100) NOT NULL,
            artist VARCHAR(100),
            album VARCHAR(100),
            genre VARCHAR(100),
            release_year INTEGER
        );
    ''')

    for row in data:
        title, artist, album, genre, release_year = row
        if not check_duplicate_song(cursor, title, artist):
            query = """
            INSERT INTO songs 
            (title, artist, album, genre, release_year) 
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (title, artist, album, genre, release_year))

    connection.commit()  # Commit the changes
    cursor.close()  # Close the cursor

def main():
    csv_file_path = 'cleaned_music_data.csv'
    database_connection = create_database_connection()
    csv_data = read_csv_data(csv_file_path)
    insert_data_into_database(database_connection, csv_data)
    database_connection.close()

if __name__ == '__main__':
    main()


