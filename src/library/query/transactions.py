from collections import defaultdict
import pymysql
from constants import db_settings

def query(user_id, table_name, filters):
    query = f"SELECT * FROM {table_name} WHERE UserID = %s"
    params = [user_id]

    filter_clauses = []
    
    for key, value in filters.items():
        if key == 'start_date':
            filter_clauses.append("Date >= %s")
            params.append(value)
        elif key == 'end_date':
            filter_clauses.append("Date <= %s")
            params.append(value)
        elif key == 'description':
            filter_clauses.append("Description LIKE %s")
            params.append(f"%{value}%")
        elif key == 'type':
            filter_clauses.append("Type = %s")
            params.append(value)
        elif key == 'amount_lt':
            filter_clauses.append("Amount < %s")
            params.append(value)
        elif key == 'amount_eq':
            filter_clauses.append("Amount = %s")
            params.append(value)
        elif key == 'amount_gt':
            filter_clauses.append("Amount > %s")
            params.append(value)
        elif key == 'balance_lt':
            filter_clauses.append("Balance < %s")
            params.append(value)
        elif key == 'balance_eq':
            filter_clauses.append("Balance = %s")
            params.append(value)
        elif key == 'balance_gt':
            filter_clauses.append("Balance > %s")
            params.append(value)
        elif key == 'category':
            filter_clauses.append("Category = %s")
            params.append(value)

    if filter_clauses:
        query += " AND " + " AND ".join(filter_clauses)

    # Connect to the database
    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Get the filtered rows
            cursor.execute(query, params)
            results = cursor.fetchall()

            # Calculate total count and total amount
            count_query = f"SELECT COUNT(*) as total_count, SUM(Amount) as total_amount FROM {table_name} WHERE UserID = %s"
            count_params = [user_id]
            if filter_clauses:
                count_query += " AND " + " AND ".join(filter_clauses)
                count_params.extend(params[1:])  # Include all filter params

            cursor.execute(count_query, count_params)
            count_result = cursor.fetchone()
            total_count = count_result['total_count']
            total_amount = count_result['total_amount'] if count_result['total_amount'] is not None else 0

            # Calculate metadata
            metadata = {
                'total_amount': total_amount,
                'total_count': total_count
            }

            # Group transactions by description and calculate sub-metadata
            grouped_results = defaultdict(lambda: {'transactions': [], 'metadata': {}})
            for item in results:
                description = item['Description']
                grouped_results[description]['transactions'].append(item)
                grouped_results[description]['metadata']['total_amount'] = grouped_results[description]['metadata'].get('total_amount', 0) + item['Amount']
                grouped_results[description]['metadata']['count'] = grouped_results[description]['metadata'].get('count', 0) + 1

            # Convert defaultdict to regular dict
            grouped_results = dict(grouped_results)

            # Create a response object
            response = {
                'metadata': metadata,
                'transactions': grouped_results
            }

    finally:
        connection.close()
    
    return response
