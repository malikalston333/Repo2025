# --------------------------
# Step 1: Import Dependecies
# --------------------------
# 'requests' is a Python library that allows us to send HTTP requests (like GET, POST).
# Access data from a public API endpoint
import requests
# 'mysql.connector is a module that lets Python connect to MySQL databse.
import mysql.connector as dbconnect

# ----------------------------------------------
# Step 2: Define Function to Fetch Data from API
# ----------------------------------------------
def fetch_posts():
    # Define the API endpoint URL. This is where the dataset is hosted online.
    url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
    # Send a GET request to the API. This asks the server to send back data.
    response = requests.get(url)
    # Check if the server responded with a success code (200 = ok)
    if response.status_code == 200:
        # If the request is successful, parse the response as JSON.
        # This returns a list of dictionaries (each representing one record).
        return response.json()
    else:
        # If the request failed, print an error message showing the status code.
        print(f"Failed to fetch data. Status code: {response.status_code}")
# --------------------------------------------
# Step 3: Define Function to Load JSON Data into MySQL
# --------------------------------------------
def load_to_mysql(data):
    try:
        # Establish Connection to MySQL
        # We provide the host, port, username, password, and target database name.
        conn = dbconnect.connect(
            host='localhost',    # Local database server
            port=3306,           # Default port for MySQL
            user='root',         # MySQL username
            password='password', # Your password
            database='creditcard_capstone' # Target database name
        )
        # Create a cursor object which allows us to execute SQL commands
        cursor = conn.cursor()

        # Create table if it doesn't exist
        # This command creates the table only if it hasn't been created already.
        # The structure includes 10 fields relevant to the loan application.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS CDW_SAPP_loan_application (
                Application_ID VARCHAR(20) PRIMARY KEY,
                Gender VARCHAR(10),
                Married VARCHAR(10),
                Dependents VARCHAR(10),
                Education VARCHAR(50),
                Self_Employed VARCHAR(10),
                Credit_History INT,
                Property_Area VARCHAR(20),
                Income VARCHAR(20),
                Application_Status VARCHAR(5)
            )
        ''')
        # Ensure Records Have All Expected Keys
        # Define the list of required keys we expect in every record.
        expected_keys = [
            'Application_ID', 'Gender', 'Married', 'Dependents',
            'Education', 'Self_Employed', 'Credit_History',
            'Property_Area', 'Income', 'Application_Status'
        ]
        # Loop Through Each Record and Insert into Table
        for info in data:
            try:
                if all(key in info for key in expected_keys):
                    # If yes, insert the data into the MySQL table
                    # Use parameterized query to prevent SQL injection
                    # ON DUPLICATE KEY UPDATE means if the primary key already exists,
                    # update the other fields instead of inserting a new row
                    cursor.execute('''
                        INSERT INTO CDW_SAPP_loan_application (
                            Application_ID, Gender, Married, Dependents, Education, 
                            Self_Employed, Credit_History, Property_Area, Income, Application_Status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            Gender=VALUES(Gender),
                            Married=VALUES(Married),
                            Dependents=VALUES(Dependents),
                            Education=VALUES(Education),
                            Self_Employed=VALUES(Self_Employed),
                            Credit_History=VALUES(Credit_History),
                            Property_Area=VALUES(Property_Area),
                            Income=VALUES(Income),
                            Application_Status=VALUES(Application_Status)
                    ''', (
                        info['Application_ID'],
                        info['Gender'],
                        info['Married'],
                        info['Dependents'],
                        info['Education'],
                        info['Self_Employed'],
                        info['Credit_History'],
                        info['Property_Area'],
                        info['Income'],
                        info['Application_Status']
                    ))
                else:
                    # If any required field is missing, skip the record and log a warning
                    print(f"Skipping record due to missing keys: {info.get('Application_ID', 'UNKNOWN')}")
            except Exception as record_error:
                # If something goes wrong with this record (e.g., wrong data type), print error
                print(f"Error inserting record {info.get('Application_ID', 'UNKNOWN')}: {record_error}")
        # Save all insert/update changes to the database
        conn.commit()
        # Close the cursor and the connection to free resources
        cursor.close()
        conn.close()
        # Print success message
        print("Data loaded successfully to MySQL database.")
    except Exception as e:
        # If the connection or table setup fails, print the error message
        print(f"Connection or setup error: {e}")
# -----------------------------
# Step 4: Run The Whole Process
# -----------------------------
# Call the function to fetch data from the API
data = fetch_posts()
# If data was returned successfully, load it into the database
if data:
    load_to_mysql(data)
