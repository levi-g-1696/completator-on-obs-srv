import pyodbc

# Get the list of available ODBC drivers
drivers = [driver for driver in pyodbc.drivers()]

# Print the list of available drivers
for driver in drivers:
    print(driver)


# Database connection settings
server = 'observationdb.cbq8ahnbfrlw.eu-north-1.rds.amazonaws.com'
database = 'observationdb'
username = 'levi'
password = 'f43r3g244rr'

# Create the connection string
cnxn = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

try:
    # Establish the database connection
    conn = pyodbc.connect(cnxn)
    cursor = conn.cursor()

    # Execute a sample query
    cursor.execute('SELECT TOP 5 * FROM a3 order by id desc')

    # Fetch and print the query results
    for row in cursor.fetchall():
        print(row)

    # Close the cursor and connection
    cursor.close()
    conn.close()

except pyodbc.Error as e:
    print(f"Database connection error: {str(e)}")
