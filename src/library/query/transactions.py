from collections import defaultdict
import pymysql
from constants import db_settings

# List of general filters that can be applied to transactions
GENERAL_FILTERS = [
    'amount_lt',
    'amount_eq',
    'amount_gt',
    'balance_lt',
    'balance_eq',
    'balance_gt',
    'category',
    'description',
    'end_date',
    'start_date',
    'type', 
]

def build_user_filter(user_id):
    """
    Ensures that the user_id condition is only applied once in queries.
    """
    return "t.UserID = %s", [user_id]

def build_query(table_name, user_id, filters):
    """
    Builds the SQL query for fetching transactions based on the filters provided.
    """
    # Get the user filter condition
    user_filter, params = build_user_filter(user_id)

    # Base query with necessary joins
    query = f"""
        SELECT 
            t.*, 
            IFNULL(ctdm.CustomDescription, t.Description) AS DisplayDescription,
            IFNULL(ucm.CategoryName, dcm.CategoryName) AS CategoryName
        FROM {table_name} t
        LEFT JOIN CustomTransactionDescriptionMapping ctdm 
            ON t.UserID = ctdm.UserID AND t.Description = ctdm.OriginalDescription
        LEFT JOIN TransactionCategoryMapping tcm 
            ON t.UserID = tcm.UserID AND t.Description = tcm.TransactionDescription
        LEFT JOIN UserCategories ucm 
            ON tcm.CategoryID = ucm.CategoryID
        LEFT JOIN DefaultCategories dcm
            ON ucm.CategoryID IS NULL AND tcm.CategoryID = dcm.CategoryID
        WHERE {user_filter}
    """
    
    filter_clauses = []

    # Add filter conditions
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
        elif key == 'category':
            categories = value.split(',')
            placeholders = ', '.join(['%s'] * len(categories))
            filter_clauses.append(f"(ucm.CategoryName IN ({placeholders}) OR dcm.CategoryName IN ({placeholders}))")
            params.extend(categories)

    if filter_clauses:
        query += " AND " + " AND ".join(filter_clauses)
    
    return query, params

def execute_query(query, params):
    """
    Executes the SQL query and returns the results.
    """
    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    finally:
        connection.close()

def calculate_total_metadata(table_name, user_id, filters):
    """
    Calculates total count and total amount for the filtered transactions.
    """
    user_filter, user_params = build_user_filter(user_id)

    count_query = f"""
        SELECT COUNT(*) as total_count, SUM(t.Amount) as total_amount 
        FROM {table_name} t
        LEFT JOIN TransactionCategoryMapping tcm 
            ON t.UserID = tcm.UserID AND t.Description = tcm.TransactionDescription
        LEFT JOIN UserCategories ucm 
            ON tcm.CategoryID = ucm.CategoryID
        LEFT JOIN DefaultCategories dcm
            ON ucm.CategoryID IS NULL AND tcm.CategoryID = dcm.CategoryID
        WHERE {user_filter}
    """
    count_params = user_params.copy()  # Use same params, skip user_id

    filter_clauses = []
    
    # Add filter conditions for the count query
    for key, value in filters.items():
        if key == 'start_date':
            filter_clauses.append("t.Date >= %s")
            count_params.append(value)
        elif key == 'end_date':
            filter_clauses.append("t.Date <= %s")
            count_params.append(value)
        elif key == 'category':
            categories = value.split(',')
            placeholders = ', '.join(['%s'] * len(categories))
            filter_clauses.append(f"(ucm.CategoryName IN ({placeholders}) OR dcm.CategoryName IN ({placeholders}))")
            count_params.extend(categories)

    if filter_clauses:
        count_query += " AND " + " AND ".join(filter_clauses)

    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(count_query, count_params)
            count_result = cursor.fetchone()
            total_count = count_result['total_count']
            total_amount = count_result['total_amount'] if count_result['total_amount'] is not None else 0
    finally:
        connection.close()

    return total_count, total_amount

def fetch_user_categories_or_defaults(user_id, account_type):
    """
    Fetches user-defined categories for the given user and account type. 
    If no user categories exist, fetches default categories for the given account type.
    """
    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Fetch user-defined categories
            cursor.execute("""
                SELECT CategoryID, CategoryName, ColorHex, Description 
                FROM UserCategories 
                WHERE UserID = %s AND AccountType = %s
            """, (user_id, account_type))
            user_categories = cursor.fetchall()

            # If no user categories are found, fetch default categories
            if not user_categories:
                cursor.execute("""
                    SELECT CategoryID, CategoryName, ColorHex, Description 
                    FROM DefaultCategories 
                    WHERE AccountType = %s
                """, (account_type,))
                user_categories = cursor.fetchall()

            return user_categories

    finally:
        connection.close()

def generate_time_series_data(table_name, user_id, filters):
    """
    Generates time series data for each category for charting purposes.
    """
    user_filter, params = build_user_filter(user_id)

    time_series_query = f"""
        SELECT 
            IFNULL(ucm.CategoryName, dcm.CategoryName) AS CategoryName, 
            t.Date, 
            SUM(t.Amount) as total_amount
        FROM {table_name} t
        LEFT JOIN TransactionCategoryMapping tcm 
            ON t.UserID = tcm.UserID AND t.Description = tcm.TransactionDescription
        LEFT JOIN UserCategories ucm 
            ON tcm.CategoryID = ucm.CategoryID
        LEFT JOIN DefaultCategories dcm
            ON ucm.CategoryID IS NULL AND tcm.CategoryID = dcm.CategoryID
        WHERE {user_filter}
    """
    filter_clauses = []

    for key, value in filters.items():
        if key == 'start_date':
            filter_clauses.append("t.Date >= %s")
            params.append(value)
        elif key == 'end_date':
            filter_clauses.append("t.Date <= %s")
            params.append(value)
    
    if filter_clauses:
        time_series_query += " AND " + " AND ".join(filter_clauses)

    time_series_query += " GROUP BY CategoryName, t.Date ORDER BY t.Date"
    time_series_data = execute_query(time_series_query, params)

    # Format the time series data
    time_series = defaultdict(lambda: defaultdict(float))
    for row in time_series_data:
        category = row['CategoryName']
        date = row['Date'].strftime('%Y-%m-%d')
        time_series[category][date] = float(row['total_amount'])

    return time_series

def group_transactions_by_description(results):
    """
    Groups transactions by their description (or custom description) and calculates sub-metadata.
    """
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
    
    return grouped_results

def apply_amount_filters(grouped_results, filters):
    """
    Applies amount filters (amount_lt, amount_eq, amount_gt) to grouped transactions.
    """
    if 'amount_lt' in filters or 'amount_eq' in filters or 'amount_gt' in filters:
        amount_filtered_results = {}
        filtered_total_amount = 0
        filtered_total_count = 0

        for description, data in grouped_results.items():
            total_amount = data['metadata']['total_amount']
            if 'amount_lt' in filters and total_amount >= float(filters['amount_lt']):
                continue
            if 'amount_eq' in filters and total_amount != float(filters['amount_eq']):
                continue
            if 'amount_gt' in filters and total_amount <= float(filters['amount_gt']):
                continue

            filtered_total_amount += total_amount
            filtered_total_count += data['metadata']['count']
            amount_filtered_results[description] = data

        return amount_filtered_results, filtered_total_amount, filtered_total_count

    return grouped_results, None, None

def query(user_id, table_name, filters, account_type):
    """
    Main function to query transactions and calculate metadata.
    """

    # Build the main query and execute it
    query_str, params = build_query(table_name, user_id, filters)
    results = execute_query(query_str, params)

    # Calculate total metadata
    total_count, total_amount = calculate_total_metadata(table_name, user_id, filters)

    # Fetch user categories or default categories
    all_categories = fetch_user_categories_or_defaults(user_id, account_type)

    # Generate time series data for charting
    time_series = generate_time_series_data(table_name, user_id, filters)

    # Group transactions by description and apply filters
    grouped_results = group_transactions_by_description(results)

    # Apply amount filters
    grouped_results, filtered_total_amount, filtered_total_count = apply_amount_filters(grouped_results, filters)

    # If amount filters were applied, update the metadata totals
    if filtered_total_amount is not None and filtered_total_count is not None:
        total_amount = filtered_total_amount
        total_count = filtered_total_count

    # Construct the final response
    metadata = {
        'total_amount': total_amount,
        'total_count': total_count,
        'all_categories': all_categories,
        'time_series': time_series
    }

    return {
        'metadata': metadata,
        'transactions': dict(grouped_results)
    }
