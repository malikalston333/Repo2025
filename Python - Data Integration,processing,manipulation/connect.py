# Import the mysql.connector module and alias it as mydbconnection
import mysql.connector as mydbconnection

# Import the Error class from the mysql.connector module to handle exceptions
from mysql.connector import Error

# Define a function named 'connect' to handle the database connection
def connect():
    """ Connect to MySQL database """
    
    # Initialize the connection object to None
    conn = None
    
    try:
        # Attempt to connect to the MySQL database with specified credentials
        conn = mydbconnection.connect(
            database='classicmodels',  # Name of the database you want to connect to
            user='root',               # MySQL username
            password='password'        # MySQL password
        )

        # Check if the connection was successful
        if conn.is_connected():
            print('Connected to MySQL database')

    # If a connection error occurs, catch it and print the error message
    except Error as e:
        print(e)

    # This block will run no matter what — used for cleanup
    finally:
        # If the connection exists and is still open, close it to free resources
        if conn is not None and conn.is_connected():
            conn.close()

# This block ensures the 'connect' function runs only if the script is executed directly
if __name__ == '__main__':
    connect()
