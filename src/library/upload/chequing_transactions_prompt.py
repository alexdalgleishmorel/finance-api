prompt = """
You are given CSV file content containing chequing account transactions. 
Your task is to process the CSV data and return a list of JSON objects, one for each transaction. 
Each JSON object should include the following keys: date, description, type, amount, balance (if it exists) and category.

The category value should be determined based on the description and must fall under one of the following categories:

- Groceries: Keywords pertaining to supermarkets, grocery stores, and similar places.
- Dining and Restaurants: Keywords pertaining to restaurants, cafes, fast food, bars, or pubs. Values with food names or liquor names would be likely candidates.
- Transportation: Keywords pertaining to public transport, taxis, ride-sharing services (Uber, Lyft), and fuel for vehicles.
- Entertainment: Keywords relating to sports activities like climbing.
- Utilities: Keywords pertaining to electricity, water, gas, internet, and phone services.
- Healthcare: Keywords relating to medical bills, pharmacy purchases, and other health-related expenses.
- Shopping: Keywords pertaining to purchases made at retail stores, online shopping, clothing, and accessories.
- Subscriptions: Keywords pertaining to monthly or yearly subscriptions to magazines, streaming services, and other recurring services.
- Insurance: Keywords pertaining to payments for health, auto, home, and other types of insurance.
- Electronics and Gadgets: Keywords pertaining to purchases of computers, smartphones, and other electronic devices.
- Financial Services: Keywords pertaining to bank fees, financial planning services, and other financial expenses.
- Unknown: Transactions that do not fit into any of the above categories.

Please read the CSV data, process each transaction, and return the list of JSON objects.

The only content of your response should be a raw, unformatted list of all the provided transactions.
The response should be able to be immediately loaded into a JSON object.
"""
