import json
import pymysql
from pymysql.cursors import DictCursor
import pandas as pd

from constants import db_settings
import library.gpt as gpt

def update_upload_progress(cursor, user_id, account_type, progress):
    """
    Updates the upload progress for the user in the shared progress table.
    Sets progress to NULL when the upload is complete.
    """
    if progress is None:
        # Reset progress to NULL after upload completes
        cursor.execute("""
            UPDATE UploadProgress 
            SET Progress = NULL
            WHERE UserID = %s AND AccountType = %s
        """, (user_id, account_type))
    else:
        # Update the progress percentage
        cursor.execute("""
            INSERT INTO UploadProgress (UserID, AccountType, Progress)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE Progress = %s
        """, (user_id, account_type, progress, progress))

def process_and_store_dataframe(user_id, dataframe, account_type):
    """
    Processes a dataframe of transactions, categorizes them using GPT where necessary, 
    and stores the data in the appropriate transaction table
    """
    processed_data = []
    new_transaction_rows = []
    new_transactions = 0
    gpt_requests = 0

    # Connect to the database
    connection = pymysql.connect(**db_settings)

    try:
        with connection.cursor(DictCursor) as cursor:
            # Check existing mappings (transaction description -> category) for the user, filtering by AccountType
            existing_mappings = get_existing_mappings(cursor, user_id, account_type)

            # Prepare data for processing (both categorized and uncategorized)
            uncategorized_rows, processed_data = process_existing_mappings(dataframe, existing_mappings)

            # Fetch user categories or default categories if user-defined ones don't exist
            category_map = get_user_categories_or_defaults(cursor, user_id, account_type)

            # Process uncategorized data with GPT in batches, updating progress after each batch
            processed_data, gpt_requests = process_uncategorized_data_with_gpt(
                cursor, uncategorized_rows, user_id, processed_data, category_map, account_type
            )

            # Insert processed transactions into the appropriate transactions table
            new_transactions, new_transaction_rows = insert_transactions(
                cursor, processed_data, user_id, account_type, new_transaction_rows, new_transactions
            )

            # Reset the upload progress to NULL after completion
            update_upload_progress(cursor, user_id, account_type, None)

        connection.commit()  # Commit the transaction to the database

    finally:
        connection.close()  # Ensure that the connection is closed

    # Return summary of transaction processing
    summary = {
        'new_transactions': new_transactions,
        'gpt_requests': gpt_requests
    }
    return new_transaction_rows, summary


def get_existing_mappings(cursor, user_id, account_type):
    """
    Fetches existing transaction category mappings for a user and account type from the database.
    """
    cursor.execute("""
        SELECT tcm.TransactionDescription, ucm.CategoryName 
        FROM TransactionCategoryMapping tcm
        LEFT JOIN UserCategories ucm ON tcm.CategoryID = ucm.CategoryID
        WHERE tcm.UserID = %s AND ucm.AccountType = %s
    """, (user_id, account_type))
    return {row['TransactionDescription']: row['CategoryName'] for row in cursor.fetchall()}


def process_existing_mappings(dataframe, existing_mappings):
    """
    Processes transactions based on existing mappings. Categorizes transactions if a category is already known.
    """
    uncategorized_rows = pd.DataFrame(columns=dataframe.columns)
    processed_data = []

    for i, row in dataframe.iterrows():
        if row['Type of Transaction'] == 'Credit':  # Ignore credit transactions like refunds
            continue
        
        description = row['Description']

        if description in existing_mappings:
            # Use the pre-existing category mapping
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
            # Add to uncategorized rows if the description is not mapped yet
            uncategorized_rows = pd.concat([uncategorized_rows, pd.DataFrame([row])], ignore_index=True)
    
    return uncategorized_rows, processed_data


def get_user_categories_or_defaults(cursor, user_id, account_type):
    """
    Fetches user-defined categories or falls back to default categories if none are found.
    """
    # First, try fetching user-defined categories
    cursor.execute("""
        SELECT CategoryID, CategoryName 
        FROM UserCategories 
        WHERE UserID = %s AND AccountType = %s
    """, (user_id, account_type))
    user_categories = {row['CategoryName']: row['CategoryID'] for row in cursor.fetchall()}

    # If user categories exist, return them
    if user_categories:
        return user_categories

    # If no user categories, fall back to default categories
    cursor.execute("""
        SELECT CategoryID, CategoryName 
        FROM DefaultCategories 
        WHERE AccountType = %s
    """, (account_type,))
    default_categories = {row['CategoryName']: row['CategoryID'] for row in cursor.fetchall()}

    return default_categories


def process_uncategorized_data_with_gpt(cursor, uncategorized_rows, user_id, processed_data, category_map, account_type):
    """
    Uses GPT to categorize uncategorized transactions and insert new category mappings.
    Also updates upload progress after each batch.
    """
    gpt_requests = 0
    total_batches = (len(uncategorized_rows) // 100) + (1 if len(uncategorized_rows) % 100 != 0 else 0)  # Calculate total batches

    for i, batch_start in enumerate(range(0, len(uncategorized_rows), 100)):  # Process in chunks of 100 rows
        chunk = uncategorized_rows[batch_start:batch_start + 100]
        chunk_str = chunk.to_csv(index=False)

        # Make GPT request to categorize transactions
        dynamic_prompt = get_user_categories_prompt(user_id, account_type)
        chunk_processed_data = json.loads(gpt.make_request(dynamic_prompt, chunk_str))
        gpt_requests += len(chunk_processed_data)

        # Add processed data to the main list and update mappings
        for row in chunk_processed_data:
            processed_data.append(row)

            # Insert new category mapping into the database
            category_id = category_map.get(row['category'])
            cursor.execute("""
                INSERT IGNORE INTO TransactionCategoryMapping 
                (UserID, TransactionDescription, CategoryID)
                VALUES (%s, %s, %s)
            """, (user_id, row['description'], category_id))

        # Update the upload progress after each batch
        progress = ((i + 1) / total_batches) * 100
        update_upload_progress(cursor, user_id, account_type, progress)

    return processed_data, gpt_requests


def insert_transactions(cursor, processed_data, user_id, account_type, new_transaction_rows, new_transactions):
    """
    Inserts the processed transactions into the respective account type's transaction table.
    """
    table_name = 'ChequingTransactions' if account_type == 'Chequing' else 'CreditTransactions'

    for row in processed_data:
        sql = f"""
        INSERT IGNORE INTO {table_name}
        (UserID, Date, Description, TransactionType, Amount, Balance) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            user_id, 
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

    return new_transactions, new_transaction_rows


def get_user_categories_prompt(user_id, account_type):
    """
    Builds the prompt for GPT based on the user's defined categories, filtering by AccountType.
    Falls back to default categories if none exist.
    """
    connection = pymysql.connect(**db_settings)

    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Fetch user-defined categories or default categories
            categories = get_user_categories_or_defaults(cursor, user_id, account_type)
    finally:
        connection.close()

    # Build the categories part of the prompt
    categories_list = "\n".join([f"- {category_name}" for category_name in categories])

    # Construct the full prompt
    prompt = f"""
    You are given CSV file content containing transactions. 
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
    """
    Wrapper function for processing and storing credit transactions.
    """
    return process_and_store_dataframe(user_id, dataframe, 'Credit')


def process_and_store_chequing_dataframe(user_id, dataframe):
    """
    Wrapper function for processing and storing chequing transactions.
    """
    return process_and_store_dataframe(user_id, dataframe, 'Chequing')
