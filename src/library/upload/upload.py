import json
import pymysql

from collections import defaultdict
import library.upload.chequing_transactions_prompt as chequing_transactions_prompt
import library.upload.credit_transactions_prompt as credit_transactions_prompt

from constants import db_settings
import library.gpt as gpt

def process_and_store_dataframe(user_id, dataframe, account_type):
    processed_data = []
    new_transactions = 0
    gpt_requests = 0

    # Connect to the database
    connection = pymysql.connect(**db_settings)

    try:
        with connection.cursor() as cursor:
            # Check existing categories and descriptions for the user
            cursor.execute(
                "SELECT TransactionDescription, CategoryName FROM TransactionCategoryMapping WHERE UserID = %s",
                (user_id,)
            )
            existing_mappings = {row['TransactionDescription']: row['CategoryName'] for row in cursor.fetchall()}

            # Prepare data for processing
            uncategorized_rows = []
            for i, row in dataframe.iterrows():
                description = row['description']
                
                # Check if the description has a pre-defined category
                if description in existing_mappings:
                    # Use existing category
                    row_data = {
                        'date': row['date'],
                        'description': description,
                        'type': row['type'],
                        'amount': row['amount'],
                        'balance': row.get('balance'),
                        'category': existing_mappings[description]
                    }
                    processed_data.append(row_data)
                else:
                    # Add row to list of uncategorized rows for processing with GPT
                    uncategorized_rows.append(row)

            # Process uncategorized data in chunks to minimize GPT requests
            for i in range(0, len(uncategorized_rows), 100):
                chunk = uncategorized_rows[i:i + 100]
                chunk_str = chunk.to_csv(index=False)
                
                # Make request to GPT to categorize transactions
                dynamic_prompt = get_user_categories_prompt(user_id)
                chunk_processed_data = json.loads(gpt.make_request(prompt, chunk_str))
                gpt_requests += len(chunk_processed_data)

                # Add processed data to main list and update mapping
                for row in chunk_processed_data:
                    processed_data.append(row)
                    # Insert the new category mapping into the database
                    cursor.execute(
                        """
                        INSERT IGNORE INTO TransactionCategoryMapping 
                        (UserID, TransactionDescription, CategoryName)
                        VALUES (%s, %s, %s)
                        """,
                        (user_id, row['description'], row['category'])
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
                new_transactions += 1

        connection.commit()

    finally:
        connection.close()

    # Summary of transaction processing
    summary = {
        'new_transactions': new_transactions,
        'gpt_requests': gpt_requests
    }

    return processed_data, summary


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
