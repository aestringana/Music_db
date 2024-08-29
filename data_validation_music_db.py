# ╔═══════════════════════════════════════════════════════════════════╗
# ║                                                                   ║
# ║                       Code by Andres Estringana                   ║
# ║                           Version 1.3                             ║
# ║    (Update: Now reads and saves CSV files to a database)          ║
# ║                                                                   ║
# ╚═══════════════════════════════════════════════════════════════════╝

import psycopg2  # Importing the PostgreSQL adapter for Python
import csv  # Importing the CSV module for reading and writing CSV files
import logging  # Importing the logging module to track events that happen during program execution

# =====================
# Logging Configuration
# =====================
# Configuring logging to write logs to 'database.log' file
# The log level is set to INFO, and logs will be appended to the file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='database.log',
    filemode='a'
)

# ============================
# Database Connection Function
# ============================
def create_database_connection():
    """
    Establishes a connection to the PostgreSQL database.
    Returns the connection object if successful, otherwise returns None.
    """
    try:
        connection = psycopg2.connect(
            dbname='music_db',
            user='postgres',
            password='1234',
            host='localhost'
        )
        logging.info("Successfully connected to the database")
        return connection
    
    # Error handling for connection failures or authentication issues
    except psycopg2.OperationalError as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

# ======================
# CSV Data Reading Function
# ======================
def read_csv_data(file_path):
    """
    Reads data from a CSV file located at 'file_path'.
    Returns a list of rows (each row is a list of values).
    """
    try:
        csv_data = []
        with open(file_path, 'r', encoding='ISO-8859-1') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header row if present
            for row in csv_reader:
                csv_data.append(row)
        
        logging.info(f"Successfully read data from CSV file: {file_path}")
        return csv_data
    
    # Error handling for issues during CSV file reading
    except Exception as e:  # Catching generic exceptions
        logging.error(f"Error reading CSV file: {file_path} - {e}")
        raise  # Re-raise the exception for higher-level handling

# ======================
# Data Validation Function
# ======================
def validate_song_data(row):
    """
    Validates individual song data in a row.
    Checks for missing fields and validates the release year.
    Returns a list of error messages if any validation fails.
    """
    title, artist, album, genre, release_year = row
    
    # Checking for missing required fields
    fields_to_check = [
        (title, "Title is missing"),
        (artist, "Artist is missing"),
        (album, "Album is missing"),
        (genre, "Genre is missing")
    ]
    errors = [error_message for field, error_message in fields_to_check if not field.strip()]
    
    # Validating the release year to ensure it's a valid integer and within a reasonable range
    try:
        release_year = int(release_year)
        if release_year < 1900 or release_year > 2100:
            errors.append("Release year is out of valid range (1900-2100)")
    except ValueError:
        errors.append("Release year is not a valid integer")
    
    return errors

# =================================
# Duplicate Song Check Function
# =================================
def check_duplicate_song(cursor, title, artist):
    """
    Checks if a song with the same title and artist already exists in the database.
    Returns True if a duplicate is found, otherwise False.
    """
    try:
        query = "SELECT COUNT(*) FROM songs WHERE title = %s AND artist = %s"
        cursor.execute(query, (title, artist))
        count = cursor.fetchone()[0]
        
        # Logging the number of duplicates found
        logging.info(f"Duplicate check: found {count} duplicates for Title='{title}', Artist='{artist}'")
        return count > 0
    
    # Error handling for issues during the database query
    except psycopg2.Error as e:
        logging.error(f"Error checking duplicate song: {e}")
        raise

# ==================================
# Database Insertion Function
# ==================================
def insert_data_into_database(connection, data):
    """
    Inserts validated song data into the database.
    Skips invalid rows and avoids inserting duplicate songs.
    """
    try:
        cursor = connection.cursor()  # Initializing cursor for database operations
        
        # Creating the 'songs' table if it does not already exist
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
        
        # Iterating over each row of data for validation and insertion
        for row in data:
            validation_errors = validate_song_data(row)
            if validation_errors:
                # Logging warnings for each invalid row and the specific errors
                logging.warning(f"Skipping invalid row: {row}")
                for error in validation_errors:
                    logging.warning(f" - {error}")
                continue  # Skip to the next row if the current one is invalid
            
            title, artist, album, genre, release_year = row
            
            # Inserting the row if it is not a duplicate
            if not check_duplicate_song(cursor, title, artist):
                query = """
                INSERT INTO songs 
                (title, artist, album, genre, release_year) 
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(query, (title, artist, album, genre, release_year))
        
        connection.commit()  # Commit the changes to the database
        logging.info("Data inserted into database successfully")
    
    # Error handling for issues during the database insertion
    except psycopg2.DatabaseError as e:
        connection.rollback()  # Rollback the transaction if an error occurs
        logging.error(f"Error inserting data into the database: {e}")
        raise
    
    finally:
        cursor.close()  # Ensure the cursor is closed after the operations

# ================
# Main Function
# ================
def main():
    """
    Main function that orchestrates the CSV data reading,
    database connection, and data insertion process.
    """
    csv_file_path = 'cleaned_music_data.csv'  # Specify the path to your CSV file
    
    try:
        database_connection = create_database_connection()  # Establish database connection
        
        if database_connection is None:
            logging.error("Failed to establish database connection. Exiting.")
            return  # Exit if the database connection fails
        
        csv_data = read_csv_data(csv_file_path)  # Read data from the CSV file
        insert_data_into_database(database_connection, csv_data)  # Insert data into the database
    
    # Catching any unexpected exceptions during the process
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    finally:
        if database_connection:
            database_connection.close()  # Close the database connection
            logging.info("Database connection closed")

# ==================
# Entry Point
# ==================
if __name__ == '__main__':
    main()  # Execute the main function
