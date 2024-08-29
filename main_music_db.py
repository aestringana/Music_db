# ╔═══════════════════════════════════════════════════════════════════╗
# ║                                                                   ║
# ║                       Code by Andres Estringana                   ║
# ║                           Version 1.3                             ║
# ║    (Update: Now reads and saves CSV files to a database)          ║
# ║                                                                   ║
# ╚═══════════════════════════════════════════════════════════════════╝

import psycopg2
import csv
import logging

# Logging settings
logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	filename='database.log',
	filemode='a'
)

def create_database_connection():
	try:
	    connection = psycopg2.connect(
	        dbname='music_db',
	        user='postgres',
	        password='1234',
	        host='localhost'
	    )
	    logging.info("Successfully connected to the database")
	    return connection
	#error handling for potential connection 
	#failures or authentication issues.
	except psycopg2.OperationalError as e:
		logging.error(f"Error connecting to the database: {e}")
		return None


def read_csv_data(file_path):
	try:
	    csv_data = []
	    with open(file_path, 'r', encoding='ISO-8859-1') as csv_file:
	        csv_reader = csv.reader(csv_file)
	        next(csv_reader)  # Skip the header row if present
	        for row in csv_reader:
	            csv_data.append(row)
	    logging.info(f"Successfully read data from CSV file: {file_path}")
	    return csv_data
	except csv_data as e:
		logging.error(f"Error reading CSV file: {file_path}")
		 raise

def check_duplicate_song(cursor, title, artist):
	try:
	    query = "SELECT COUNT(*) FROM songs WHERE title = %s AND artist = %s"
	    cursor.execute(query, (title, artist))
	    count = cursor.fetchone()[0]
	    return count > 0
	    logging.info (f"""
	    	There was {count} songs duplicated. 
	    	Duplicate song found: 
	    	Title='{title}', Artist='{artist}'
	    """)
	except psycopg2.Error as e:
		logging.error(f"Error checking duplicate song: {e}")
		raise

def insert_data_into_database(connection, data):
	try:
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
	    logging.info("Data inserted into database Successfully")
	except psycopg2.DatabaseError as e:
		connection.rollback()
		logging.error(f"Error inserting data into the database: {e}")
		raise
	finally:  # Close the cursor
		cursor.close() 

def main():
    csv_file_path = 'cleaned_music_data.csv'
    try:
    	database_connection = create_database_connection()
    	if database_connection is None:
    		logging.error("Failed to establish database connection. Exiting.")
    		return 
    	csv_data = read_csv_data(csv_file_path)
    	insert_data_into_database(database_connection, csv_data)
    except Exception as e:
    	logging.error(f"An error occurred: {e}")
    finally:
    	if database_connection:
    		database_connection.close()
    		logging.info("Database connection closed")

if __name__ == '__main__':
    main()


