import json
import pymysql
from pymysql.cursors import DictCursor
import pandas as pd

from collections import defaultdict
import library.upload.chequing_transactions_prompt as chequing_transactions_prompt
import library.upload.credit_transactions_prompt as credit_transactions_prompt

from constants import db_settings
import library.gpt as gpt

def process_and_store_dataframe(user_id, dataframe, account_type):
    processed_data = []
    new_transaction_rows = []
    new_transactions = 0
    gpt_requests = 0

    # Connect to the database
    connection = pymysql.connect(**db_settings)

    try:
        with connection.cursor(DictCursor) as cursor:
            # Check existing categories and descriptions for the user
            cursor.execute(
                """
                SELECT tcm.TransactionDescription, ucm.CategoryName 
                FROM TransactionCategoryMapping tcm
                LEFT JOIN UserCategories ucm ON tcm.CategoryID = ucm.CategoryID
                WHERE tcm.UserID = %s
                """,
                (user_id,)
            )
            existing_mappings = {row['TransactionDescription']: row['CategoryName'] for row in cursor.fetchall()}

            # Prepare data for processing
            uncategorized_rows = pd.DataFrame(columns=dataframe.columns)

            for i, row in dataframe.iterrows():
                if row['Type of Transaction'] == 'Credit':
                    # Ignoring refunds or payments to the credit card
                    continue

                description = row['Description']
                
                # Check if the description has a pre-defined category
                if description in existing_mappings:
                    # Use existing category
                    row_data = {
                        'date': row['Date'],
                        'description': description,
                        'type': row['Type of Transaction'],
                        'amount': row['Amount'],
                        'balance': row.get('Balance'),
                        'category': existing_mappings[description]
                    }
                    processed_data.append(row_data)
                else:
                    # Add row to list of uncategorized rows for processing with GPT
                    uncategorized_rows = pd.concat([uncategorized_rows, pd.DataFrame([row])], ignore_index=True)

            # Fetch all categories and their IDs once at the beginning, which will be used later for inserting transactions
            cursor.execute(
                "SELECT CategoryID, CategoryName FROM UserCategories WHERE UserID = %s",
                (user_id,)
            )
            category_map = {row['CategoryName']: row['CategoryID'] for row in cursor.fetchall()}

            # Process uncategorized data in chunks to minimize GPT requests
            for i in range(0, len(uncategorized_rows), 100):
                chunk = uncategorized_rows[i:i + 100]
                chunk_str = chunk.to_csv(index=False)
                
                # Make request to GPT to categorize transactions
                dynamic_prompt = get_user_categories_prompt(user_id)
                chunk_processed_data = json.loads(gpt.make_request(dynamic_prompt, chunk_str))
                gpt_requests += len(chunk_processed_data)

                # Add processed data to main list and update mapping
                for row in chunk_processed_data:
                    processed_data.append(row)

                    # Retrieve CategoryID from the pre-fetched category map
                    category_id = category_map.get(row['category'])

                    # Insert the new category mapping into the database using CategoryID
                    cursor.execute(
                        """
                        INSERT IGNORE INTO TransactionCategoryMapping 
                        (UserID, TransactionDescription, CategoryID)
                        VALUES (%s, %s, %s)
                        """,
                        (user_id, row['description'], category_id)
                    )

            # Insert processed transactions into the Transactions table
            for row in processed_data:
                sql = """
                INSERT IGNORE INTO Transactions 
                (UserID, AccountType, Date, Description, TransactionType, Amount, Balance) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    user_id, 
                    account_type, 
                    row['date'], 
                    row['description'], 
                    row['type'], 
                    row['amount'], 
                    row.get('balance')
                ))
                # Only count as new if the row was actually inserted
                if cursor.rowcount > 0:
                    new_transaction_rows.append(row)
                    new_transactions += 1

        connection.commit()

    finally:
        connection.close()

    # Summary of transaction processing
    summary = {
        'new_transactions': new_transactions,
        'gpt_requests': gpt_requests
    }

    return new_transaction_rows, summary


def get_user_categories_prompt(user_id):
    # Connect to the database
    connection = pymysql.connect(**db_settings)
    
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Fetch user-defined categories and descriptions
            cursor.execute(
                "SELECT CategoryName, Description FROM UserCategories WHERE UserID = %s",
                (user_id,)
            )
            categories = cursor.fetchall()

    finally:
        connection.close()

    # Build the categories part of the prompt
    categories_list = "\n".join(
        [f"- {row['CategoryName']}: {row['Description']}" for row in categories]
    )

    # Construct the complete prompt dynamically
    prompt = f"""
    You are given CSV file content containing credit card transactions. 
    Your task is to process the CSV data and return a list of JSON objects, one for each transaction.
    Transactions of type 'Credit' should be ignored and not returned as part of the response.
    Each JSON object should include the following keys: date, description, type, amount, and category.

    The category value should be determined based on the description and must fall under one of the following categories:

    {categories_list}

    Please read the CSV data, process each transaction, and return the list of JSON objects.

    The only content of your response should be a raw, unformatted list of all the provided transactions.
    The response should be able to be immediately loaded into a JSON object.
    """
    
    return prompt


def process_and_store_credit_dataframe(user_id, dataframe):
    return process_and_store_dataframe(user_id, dataframe, 'Credit')


def process_and_store_chequing_dataframe(user_id, dataframe):
    return process_and_store_dataframe(user_id, dataframe, 'Chequing')
