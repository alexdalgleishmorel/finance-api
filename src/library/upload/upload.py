import json
import pymysql

import library.upload.chequing_transactions_prompt as chequing_transactions_prompt
import library.upload.credit_transactions_prompt as credit_transactions_prompt

from constants import db_settings
import library.gpt as gpt


def process_and_store_credit_dataframe(user_id, dataframe):
    processed_data = []

    # Process the data in chunks of 100 rows
    for i in range(0, len(dataframe), 100):
        chunk = dataframe.iloc[i:i + 100]
        chunk_str = chunk.to_csv(index=False)
        chunk_processed_data = json.loads(gpt.make_request(credit_transactions_prompt.prompt, chunk_str))
        processed_data.extend(chunk_processed_data)

    # Connect to the database
    connection = pymysql.connect(**db_settings)
    
    try:
        with connection.cursor() as cursor:
            for row in processed_data:
                sql = "INSERT IGNORE INTO CreditTransactions (UserID, Date, Description, Type, Amount, Category) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (user_id, row['date'], row['description'], row['type'], row['amount'], row['category']))
        
        connection.commit()
    finally:
        connection.close()

    return processed_data


def process_and_store_chequing_dataframe(user_id, dataframe):
    processed_data = []

    # Process the data in chunks of 100 rows
    for i in range(0, len(dataframe), 100):
        chunk = dataframe.iloc[i:i + 100]
        chunk_str = chunk.to_csv(index=False)
        chunk_processed_data = json.loads(gpt.make_request(chequing_transactions_prompt.prompt, chunk_str))
        processed_data.extend(chunk_processed_data)

    # Connect to the database
    connection = pymysql.connect(**db_settings)
    
    try:
        with connection.cursor() as cursor:
            for row in processed_data:
                sql = "INSERT IGNORE INTO ChequingTransactions (UserID, Date, Description, Type, Amount, Balance, Category) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (user_id, row['date'], row['description'], row['type'], row['amount'], row['balance'], row['category']))

        connection.commit()
    finally:
        connection.close()

    return processed_data
