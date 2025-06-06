# Importing necessary functions from project modules
from etl import create_tables, pipeline           # ETL setup and data loading
from api import fetch_posts, load_to_mysql        # API fetch and database insert
from cli import run_cli                           # Console menu interface

# Main execution block
if __name__ == "__main__":
    # Step 1: Create necessary tables in the MySQL database
    create_tables()

    # Step 2: Run the ETL pipeline to load and transform initial data
    pipeline()

    # Step 3: Fetch external JSON data from the API and load it into MySQL
    data = fetch_posts()
    if data:
        load_to_mysql(data)

    # Step 4: Launch the interactive command-line interface (CLI) for user interaction
    run_cli()
