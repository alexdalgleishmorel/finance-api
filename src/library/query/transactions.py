from collections import defaultdict
import pymysql
from constants import db_settings

def query(user_id, table_name, filters):
    # Base query with necessary joins to handle categories and custom descriptions
    query = f"""
        SELECT 
            t.*, 
            IFNULL(ctdm.CustomDescription, t.Description) AS DisplayDescription,
            ucm.CategoryName 
        FROM {table_name} t
        LEFT JOIN CustomTransactionDescriptionMapping ctdm 
            ON t.UserID = ctdm.UserID AND t.Description = ctdm.OriginalDescription
        LEFT JOIN TransactionCategoryMapping tcm 
            ON t.UserID = tcm.UserID AND t.Description = tcm.TransactionDescription
        LEFT JOIN UserCategories ucm 
            ON tcm.CategoryID = ucm.CategoryID
        WHERE t.UserID = %s
    """
    params = [user_id]

    filter_clauses = []

    # Handling filters
    for key, value in filters.items():
        if key == 'start_date':
            filter_clauses.append("t.Date >= %s")
            params.append(value)
        elif key == 'end_date':
            filter_clauses.append("t.Date <= %s")
            params.append(value)
        elif key == 'description':
            filter_clauses.append("t.Description LIKE %s")
            params.append(f"%{value}%")
        elif key == 'type':
            filter_clauses.append("t.TransactionType = %s")
            params.append(value)
        elif key == 'amount_lt':
            filter_clauses.append("t.Amount < %s")
            params.append(value)
        elif key == 'amount_eq':
            filter_clauses.append("t.Amount = %s")
            params.append(value)
        elif key == 'amount_gt':
            filter_clauses.append("t.Amount > %s")
            params.append(value)
        elif key == 'balance_lt':
            filter_clauses.append("t.Balance < %s")
            params.append(value)
        elif key == 'balance_eq':
            filter_clauses.append("t.Balance = %s")
            params.append(value)
        elif key == 'balance_gt':
            filter_clauses.append("t.Balance > %s")
            params.append(value)
        elif key == 'category':
            filter_clauses.append("ucm.CategoryName in (%s)")
            params.append(value)

    # Apply filters to the query
    if filter_clauses:
        query += " AND " + " AND ".join(filter_clauses)

    # Connect to the database
    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Execute the filtered query
            cursor.execute(query, params)
            results = cursor.fetchall()

            # Calculate total count and total amount based on the filtered transactions
            count_query = f"""
                SELECT COUNT(*) as total_count, SUM(t.Amount) as total_amount 
                FROM {table_name} t
                LEFT JOIN TransactionCategoryMapping tcm 
                    ON t.UserID = tcm.UserID AND t.Description = tcm.TransactionDescription
                LEFT JOIN UserCategories ucm 
                    ON tcm.CategoryID = ucm.CategoryID
                WHERE t.UserID = %s
            """
            count_params = [user_id]
            if filter_clauses:
                count_query += " AND " + " AND ".join(filter_clauses)
                count_params.extend(params[1:])  # Include all filter params

            cursor.execute(count_query, count_params)
            count_result = cursor.fetchone()
            total_count = count_result['total_count']
            total_amount = count_result['total_amount'] if count_result['total_amount'] is not None else 0

            # Fetch all categories defined by the user
            cursor.execute(
                "SELECT CategoryName FROM UserCategories WHERE UserID = %s", 
                [user_id]
            )
            all_categories = [row['CategoryName'] for row in cursor.fetchall()]

            # Calculate metadata
            metadata = {
                'total_amount': total_amount,
                'total_count': total_count,
                'all_categories': all_categories
            }

            # Group transactions by custom or original description and calculate sub-metadata
            grouped_results = defaultdict(lambda: {'transactions': [], 'metadata': {}})
            for item in results:
                description = item['DisplayDescription']
                grouped_results[description]['transactions'].append({
                    **item,
                    'Amount': round(float(item['Amount']), 2)
                })
                grouped_results[description]['metadata']['total_amount'] = round(
                    float(grouped_results[description]['metadata'].get('total_amount', 0)) + float(item['Amount']), 2
                )
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
