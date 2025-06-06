import mysql.connector as mydbconnection
from mysql.connector import Error

# --------------------------------------------------
# Function: connect_to_db
# Purpose: Establish and return a connection to the MySQL database
# --------------------------------------------------
def connect_to_db():
    return mydbconnection.connect(
        host="localhost",
        port="3306",
        user="root",
        password="password",
        database="creditcard_capstone"
    )

# --------------------------------------------------
# Function: transaction
# Purpose: Prompt for ZIP/month/year and retrieve matching transactions
# --------------------------------------------------
def transaction():
    while True:
        zip_code = input("Please enter your 5-digit ZIP Code (e.g., 12345): ")
        if len(zip_code) == 5 and zip_code.isdigit():
            break
        else:
            print("Invalid ZIP code. Try again.")

    while True:
        month_input = input("Please enter the month (MM format, e.g., 03): ")
        if len(month_input) == 2 and month_input.isdigit() and 1 <= int(month_input) <= 12:
            break
        else:
            print("Invalid month. Enter a value between 01 and 12.")

    while True:
        year_input = input("Please enter the year (YYYY format, e.g., 2022): ")
        if len(year_input) == 4 and year_input.isdigit():
            break
        else:
            print("Invalid year. Enter a valid 4-digit year.")

    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                cc.TRANSACTION_ID,
                cc.CREDIT_CARD_NO,
                cc.TRANSACTION_TYPE,
                cc.TRANSACTION_VALUE,
                cc.DAY,
                cc.MONTH,
                cc.YEAR
            FROM CDW_SAPP_CREDIT_CARD cc
            JOIN CDW_SAPP_CUSTOMER c ON cc.CUST_SSN = c.SSN
            WHERE c.CUST_ZIP = %s AND cc.MONTH = %s AND cc.YEAR = %s
            ORDER BY cc.DAY DESC;
        """
        cursor.execute(query, (zip_code, int(month_input), int(year_input)))
        results = cursor.fetchall()

        if results:
            print(f"\nTransactions for ZIP {zip_code} in {month_input}/{year_input}:\n")
            for row in results:
                print(
                    f"Transaction ID: {row['TRANSACTION_ID']}, "
                    f"Card No: {row['CREDIT_CARD_NO']}, "
                    f"Type: {row['TRANSACTION_TYPE']}, "
                    f"Amount: ${row['TRANSACTION_VALUE']:.2f}, "
                    f"Date: {row['MONTH']:02d}/{row['DAY']:02d}/{row['YEAR']}"
                )
        else:
            print("No transactions found for that ZIP code and date.")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# --------------------------------------------------
# Function: customer_details
# Purpose: Display customer account details given SSN
# --------------------------------------------------
def customer_details():
    while True:
        ssn_input = input("Enter customer's SSN (9 digits, e.g., 123456789): ")
        if len(ssn_input) == 9 and ssn_input.isdigit():
            break
        else:
            print("Invalid SSN. Please try again.")

    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT *
            FROM CDW_SAPP_CUSTOMER
            WHERE SSN = %s;
        """
        cursor.execute(query, (ssn_input,))
        result = cursor.fetchone()

        if result:
            print(f"\nHere are the Account Details for {ssn_input}:\n")
            full_name = f"{result['FIRST_NAME']} {result['MIDDLE_NAME']} {result['LAST_NAME']}".strip()
            full_address = f"{result['STREET_NAME']}, Apt {result['APT_NO']}" if result['APT_NO'] else result['STREET_NAME']
            location = f"{result['CUST_CITY']}, {result['CUST_STATE']} {result['CUST_ZIP']}"

            print(f"Name       : {full_name}")
            print(f"Address    : {full_address}")
            print(f"Location   : {location}")
            print(f"Phone      : {result['CUST_PHONE']}")
            print(f"Email      : {result['CUST_EMAIL']}")
            print(f"Last Update: {result['LAST_UPDATED']}")

        else:
            print("No customer found with that SSN.")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# --------------------------------------------------
# Function: modify_customer_details
# Purpose: Modify customer account details
# --------------------------------------------------
def modify_customer_details():
    while True:
        ssn_input = input("Enter customer's SSN (9 digits, e.g., 123456789): ")
        if len(ssn_input) == 9 and ssn_input.isdigit():
            break
        else:
            print("Invalid SSN. Please try again.")

    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)

        query = "SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = %s;"
        cursor.execute(query, (ssn_input,))
        customer = cursor.fetchone()

        if not customer:
            print("No customer found with that SSN.")
            return

        print("\nCurrent details (press Enter to keep current value):")
        fields_to_update = {}
        for field in ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'APT_NO', 'STREET_NAME', 'CUST_CITY', 'CUST_STATE', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL']:
            current_value = customer[field] if customer[field] is not None else ''
            new_value = input(f"{field.replace('_', ' ').title()} [{current_value}]: ").strip()
            if new_value != '':
                fields_to_update[field] = new_value

        if not fields_to_update:
            print("No changes made.")
            return

        set_clause = ", ".join(f"{field} = %s" for field in fields_to_update)
        values = list(fields_to_update.values())
        values.append(ssn_input)

        update_query = f"UPDATE CDW_SAPP_CUSTOMER SET {set_clause} WHERE SSN = %s;"
        cursor.execute(update_query, tuple(values))
        conn.commit()

        print("Customer details updated successfully.")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# --------------------------------------------------
# Function: generate_monthly_bill
# Purpose: Generate monthly bill for a credit card
# --------------------------------------------------
def generate_monthly_bill():
    cc_no = input("Enter Credit Card Number (e.g., 1234-5678-9012-3456): ").strip()

    while True:
        month_input = input("Enter month (2 digits, e.g., 03): ")
        if len(month_input) == 2 and month_input.isdigit() and 1 <= int(month_input) <= 12:
            break
        else:
            print("Invalid month. Please enter 01-12.")

    while True:
        year_input = input("Enter year (4 digits, e.g., 2023): ")
        if len(year_input) == 4 and year_input.isdigit():
            break
        else:
            print("Invalid year. Please enter a 4-digit year.")

    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        query = """
            SELECT SUM(TRANSACTION_VALUE)
            FROM CDW_SAPP_CREDIT_CARD
            WHERE CREDIT_CARD_NO = %s AND MONTH = %s AND YEAR = %s;
        """
        cursor.execute(query, (cc_no, int(month_input), int(year_input)))
        total = cursor.fetchone()[0]

        if total is None:
            total = 0.0

        print(f"\nTotal bill for Credit Card {cc_no} for {month_input}/{year_input}: ${total:.2f}")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# --------------------------------------------------
# Function: display_transactions_between_dates
# Purpose: Display customer transactions between two dates
# --------------------------------------------------
def display_transactions_between_dates():
    while True:
        ssn_input = input("Enter customer's SSN (9 digits): ")
        if len(ssn_input) == 9 and ssn_input.isdigit():
            break
        else:
            print("Invalid SSN. Please try again.")

    def get_date(prompt):
        while True:
            y = input(f"Enter {prompt} year (4 digits): ")
            m = input(f"Enter {prompt} month (2 digits): ")
            d = input(f"Enter {prompt} day (2 digits): ")
            if (len(y) == 4 and y.isdigit() and
                len(m) == 2 and m.isdigit() and 1 <= int(m) <= 12 and
                len(d) == 2 and d.isdigit() and 1 <= int(d) <= 31):
                return (int(y), int(m), int(d))
            else:
                print("Invalid date format. Please try again.")

    start_year, start_month, start_day = get_date("start")
    end_year, end_month, end_day = get_date("end")

    try:
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT cc.TRANSACTION_ID, cc.CREDIT_CARD_NO, cc.TRANSACTION_TYPE, cc.TRANSACTION_VALUE,
                   cc.DAY, cc.MONTH, cc.YEAR
            FROM CDW_SAPP_CREDIT_CARD cc
            JOIN CDW_SAPP_CUSTOMER c ON cc.CUST_SSN = c.SSN
            WHERE c.SSN = %s
            AND (YEAR > %s OR (YEAR = %s AND MONTH > %s) OR (YEAR = %s AND MONTH = %s AND DAY >= %s))
            AND (YEAR < %s OR (YEAR = %s AND MONTH < %s) OR (YEAR = %s AND MONTH = %s AND DAY <= %s))
            ORDER BY cc.YEAR DESC, cc.MONTH DESC, cc.DAY DESC;
        """
        cursor.execute(query, (
            ssn_input,
            start_year, start_year, start_month, start_year, start_month, start_day,
            end_year, end_year, end_month, end_year, end_month, end_day
        ))

        transactions = cursor.fetchall()

        if not transactions:
            print("No transactions found in the specified date range.")
            return

        print(f"\nTransactions for SSN {ssn_input} between {start_year}-{start_month:02d}-{start_day:02d} and {end_year}-{end_month:02d}-{end_day:02d}:\n")

        for row in transactions:
            print(
                f"Transaction ID: {row['TRANSACTION_ID']}, "
                f"Card No: {row['CREDIT_CARD_NO']}, "
                f"Type: {row['TRANSACTION_TYPE']}, "
                f"Amount: ${row['TRANSACTION_VALUE']:.2f}, "
                f"Date: {row['YEAR']:04d}-{row['MONTH']:02d}-{row['DAY']:02d}"
            )

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# --------------------------------------------------
# Function: customer
# Purpose: Customer menu for account related operations
# --------------------------------------------------
def customer():
    while True:
        print(
            "\n--- Customer Menu ---"
            "\n1. Account Details"
            "\n2. Modify Account Details"
            "\n3. Generate Monthly Bill"
            "\n4. View Transactions Between Dates"
            "\n5. Return to Main Menu"
        )
        choice = input("Please Enter Your Selection (1,2,3,4 or 5): ")

        match choice:
            case "1":
                print("\n--- Account Details ---")
                customer_details()
            case "2":
                print("\n--- Modify Account Details ---")
                modify_customer_details()
            case "3":
                print("\n--- Generate Monthly Bill ---")
                generate_monthly_bill()
            case "4":
                print("\n--- Transactions Between Dates ---")
                display_transactions_between_dates()
            case "5":
                print("Returning to Main Menu...")
                break
            case _:
                print("Invalid choice. Please try again.")

# --------------------------------------------------
# Function: main
# Purpose: Display main menu and handle user input
# --------------------------------------------------
def run_cli():
    while True:
        print(
            "\n--- Main Menu ---"
            "\n1. Transactions"
            "\n2. Customer"
            "\n3. Quit"
        )

        choice = input("Please Enter Your Selection (1, 2, or 3): ")

        match choice:
            case "1":
                print("\n--- Transactions Details ---")
                transaction()
            case "2":
                print("\n--- Customer Details ---")
                customer()
            case "3":
                print("Goodbye!")
                break
            case _:
                print("Invalid choice. Please try again.")

# --------------------------------------------------
# Entry Point
# --------------------------------------------------
if __name__ == "__main__":
    run_cli()
