import psycopg2


#***Connecting to the PostgreSQL database***
# function provided by the psycopg2 library 
#to establish a connection to a PostgreSQL database.

conn = psycopg2.connect(
	dbname='music_db',
	user='postgres',
	password='1234',
	host='localhost'
)

#***Creating a cursor object***
#object that allows executing SQL 
#statements and fetching results 
#from the database.

cursor = conn.cursor() 

#***Creating the 'songs' table***
#method of the cursor object 
#is used to execute an SQL statement.
# added IF NOT EXISTS to the CREATE TABLE statement to avoid errors 
#if the table already exists.

cursor.execute('''
	CREATE TABLE  IF NOT EXISTS songs (
		id SERIAL PRIMARY KEY,
		title VARCHAR (100) NOT NULL,
		artist VARCHAR (100),
		album VARCHAR (100),
		release_year INTEGER
	);
''')

#***Committing the changes***
# method is called on the connection
#object (conn) to commit the changes made 
#to the database.
conn.commit()

#Inserting sample data into the 'songs' table
songs_data = [
	('Blinding Lights', 'The Weeknd', 'After Hours', 2020),
    ('Levitating', 'Dua Lipa', 'Future Nostalgia', 2020),
    ('Watermelon Sugar', 'Harry Styles', 'Fine Line', 2019),
    ('Drivers License', 'Olivia Rodrigo', 'SOUR', 2021),
    ('Peaches', 'Justin Bieber', 'Justice', 2021),
    ('Savage Love', 'Jawsh 685 & Jason Derulo', 'Savage Love (Laxed – Siren Beat)', 2020),
    ('Montero (Call Me By Your Name)', 'Lil Nas X', 'Montero', 2021),
    ('Good 4 U', 'Olivia Rodrigo', 'SOUR', 2021),
    ('Bad Guy', 'Billie Eilish', 'When We All Fall Asleep, Where Do We Go?', 2019),
    ('Rockstar', 'DaBaby feat. Roddy Ricch', 'Blame It on Baby', 2020)
]
#Fuction Definitions
def check_duplicate_song(cursor, title, artist):
	query = "SELECT COUNT(*) FROM songs WHERE title = %s AND artist = %s;"
	cursor.execute(query,(title, artist))
	count = cursor.fetchone()[0]
	return count > 0

# Insert new songs only if they do not already exist in the database
#Create 'songs_data' contains tuples representing the songs to be inserted
#into the data base.
for song in songs_data:
	#Unpack tutple 'song' for eassier acces.
	title, artist, album, release_year = song
	if not check_duplicate_song(cursor, title, artist):
		cursor.execute('''
			INSERT INTO songs (title, artist, album, release_year)
			VALUES (%s, %s, %s, %s)
			''', song)
	else: 
			print(f'''Song "{title}" by "{artist}" already exist in the database.\n''')

#Committing the changes
#After inserting the sample data, the commit() 
#method is called again to commit the changes to the database.

conn.commit()

#Retrieve the data you want to 
#visualize by executing SQL queries using the cursor object.



#Closing the cursor and the database connection.

cursor.close()
conn.close()

