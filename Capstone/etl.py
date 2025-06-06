# Import neccessary modules
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, lower, concat_ws, format_string, col, lpad, lit, concat, when

# Define the JDBC MySQL connection URL
mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
# Define the configuration dictionary for MySQL connection
mysql_config = {
    "user": "root",   # MySQL username
    "password": "password",   # MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"   # JDBC driver for MySQL
}

def create_tables():
    """
    Connects to the MySQL database and creates tables if they don't exist.
    Uses SQL commands with 'CREATE TABLE IF NOT EXISTS' to avoid errors
    if the tables are already created.
    """
    # Establish connection to MySQL database with given credentials and database name
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='password',
        database='creditcard_capstone'
    )
    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Create CDW_SAPP_CUSTOMER table with specified columns and constraints
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS CDW_SAPP_CUSTOMER (
        APT_NO VARCHAR(10),
        CREDIT_CARD_NO VARCHAR(20),
        CUST_CITY VARCHAR(50),
        CUST_COUNTRY VARCHAR(50),
        CUST_EMAIL VARCHAR(100),
        CUST_PHONE VARCHAR(20),
        CUST_STATE VARCHAR(50),
        CUST_ZIP VARCHAR(10),
        FIRST_NAME VARCHAR(50),
        LAST_NAME VARCHAR(50),
        LAST_UPDATED VARCHAR(50),
        MIDDLE_NAME VARCHAR(50),
        SSN BIGINT PRIMARY KEY,
        STREET_NAME VARCHAR(100),
        ADDRESS VARCHAR(200) NOT NULL
    );
    """)

    # Create CDW_SAPP_BRANCH table with specified columns and constraints
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS CDW_SAPP_BRANCH (
        BRANCH_CITY VARCHAR(50),
        BRANCH_CODE BIGINT PRIMARY KEY,
        BRANCH_NAME VARCHAR(100),
        BRANCH_PHONE VARCHAR(20),
        BRANCH_STATE VARCHAR(50),
        BRANCH_STREET VARCHAR(100),
        BRANCH_ZIP VARCHAR(10),
        LAST_UPDATED VARCHAR(50)
    );
    """)

    # Create CDW_SAPP_CREDIT_CARD table with specified columns and constraints
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS CDW_SAPP_CREDIT_CARD (
        BRANCH_CODE BIGINT,
        CREDIT_CARD_NO VARCHAR(20),
        CUST_SSN BIGINT,
        DAY INT,
        MONTH INT,
        TRANSACTION_ID BIGINT PRIMARY KEY,
        TRANSACTION_TYPE VARCHAR(50),
        TRANSACTION_VALUE DOUBLE,
        YEAR INT,
        TIMEID VARCHAR(20) NOT NULL
    );
    """)

    # Commit the transaction to apply changes in the database
    conn.commit()
    # Close the connection to free resources
    conn.close()
    # Print confirmation that tables have been created or already exist
    print("Tables created (if not already there).")

def pipeline():
    """
    Extract, transform, and load credit-card data from JSON files into MySQL.

    This pipeline:
      * Creates a Spark session.
      * Reads the branch, credit-card, and customer JSON files (multiline-aware).
      * Cleans and reformats data (capitalization, phone masking, ZIP defaulting, date key creation).
      * Writes the resulting DataFrames to the MySQL tables
        ``CDW_SAPP_CUSTOMER``, ``CDW_SAPP_BRANCH``, and ``CDW_SAPP_CREDIT_CARD``.
    """
    # Create or retrieve an existing SparkSession.
    # This is the entry point to use DataFrame and SQL functionality in PySpark.
    # The 'appName' is used to name your Spark application, which helps when monitoring jobs in Spark UI.
    spark = SparkSession.builder.appName('Credit Card Data Loader').getOrCreate()

    # Load the 'cdw_sapp_branch.json' JSON file into a DataFrame.
    # Load the 'cdw_sapp_credit.json' JSON file into a DataFrame.
    # Load the 'cdw_sapp_customer.json' JSON file into a DataFrame.
    # The 'option("multiline", "true")' tells Spark to handle JSON entries that span multiple lines.
    # This is useful when each record is formatted across several lines for readability.
    df_branch = spark.read.option("multiline", "true").json(r"C:\Users\malik.alston\Desktop\Data\Capstone\Credit Card Dataset Overview\cdw_sapp_branch.json")
    df_cc = spark.read.option("multiline", "true").json(r"C:\Users\malik.alston\Desktop\Data\Capstone\Credit Card Dataset Overview\cdw_sapp_credit.json")
    df_customer = spark.read.option("multiline", "true").json(r"C:\Users\malik.alston\Desktop\Data\Capstone\Credit Card Dataset Overview\cdw_sapp_customer.json")

    # The below lines are currently commented out but can be used to visually inspect
    # The contents of each DataFrame. They display the top 20 rows by default in a tabular format.
    # df_customer.show()
    # df_branch.show()
    # df_cc.show()
    
    # Transform customer DataFrame columns:
    # - initcap() capitalizes the first letter of each word in names
    # - lower() converts middle name to lowercase
    # - concat_ws() concatenates STREET_NAME and APT_NO with a comma separator to form ADDRESS
    # - Mask phone number by replacing first 3 digits with (XXX), keeping last 4 digits intact
    df_customer = df_customer.withColumn("FIRST_NAME", initcap("FIRST_NAME"))
    df_customer = df_customer.withColumn("LAST_NAME", initcap("LAST_NAME"))
    df_customer = df_customer.withColumn("MIDDLE_NAME", lower("MIDDLE_NAME"))
    df_customer = df_customer.withColumn("ADDRESS", concat_ws(",", "STREET_NAME", "APT_NO"))
    df_customer = df_customer.withColumn("CUST_PHONE", concat(lit("(XXX)"), col("CUST_PHONE").substr(1, 3), lit("-"), col("CUST_PHONE").substr(4, 4)))

    # Transform branch DataFrame columns:
    # - Mask branch phone similarly to customer phone
    # - If BRANCH_ZIP is null, replace it with default value '99999'
    df_branch = df_branch.withColumn("BRANCH_PHONE", concat(lit("(XXX)"), col("BRANCH_PHONE").substr(1, 3), lit("-"), col("BRANCH_PHONE").substr(4, 4)))
    df_branch = df_branch.withColumn("BRANCH_ZIP", when(col("BRANCH_ZIP").isNull(), lit("99999")).otherwise(col("BRANCH_ZIP")))

    # Transform credit card DataFrame columns:
    # - Create TIMEID column by formatting YEAR, MONTH, DAY as YYYYMMDD string
    df_cc = df_cc.withColumn("TIMEID", format_string("%04d%02d%02d", col("YEAR"), col("MONTH"), col("DAY")))

    # The below lines are currently commented out but can be used to visually inspect
    # The contents of each DataFrame. They display the top 20 rows by default in a tabular format.
    # df_customer.show()
    # df_branch.show()
    # df_cc.show()

    # Write the DataFrame 'df_customer' to the MySQL table 'CDW_SAPP_CUSTOMER'
    # 'mode="append"' ensures that data will be added to the existing table without overwriting it
    df_customer.write.jdbc(
        url=mysql_url,   # JDBC connection URL
        table="CDW_SAPP_CUSTOMER",   # Destination table name in MySQL
        mode="append",  # use 'append' if you want to preserve existing data
        properties=mysql_config   # Pass the MySQL configuration
    )
    # Write the DataFrame 'df_branch' to the MySQL table 'CDW_SAPP_BRANCH'
    # Again, 'append' mode is used to add data without deleting existing records
    df_branch.write.jdbc(
        url=mysql_url,
        table="CDW_SAPP_BRANCH",
        mode="append",
        properties=mysql_config
    )
    # Write the DataFrame 'df_cc' to the MySQL table 'CDW_SAPP_CREDIT_CARD'
    # This transfers credit card transaction data to the specified table
    df_cc.write.jdbc(
        url=mysql_url,
        table="CDW_SAPP_CREDIT_CARD",
        mode="append",
        properties=mysql_config
    )
    # Confirmation message to indicate all DataFrames have been written to MySQL
    print("All DataFrames transferred to MySQL.")


# If this script is run directly (not imported as a module),
# then create the tables and run the ETL pipeline.
if __name__ == "__main__":
    # Create the required MySQL tables if they don't exist yet
    create_tables()
    # Run the ETL pipeline to load JSON data into MySQL tables
    pipeline()
