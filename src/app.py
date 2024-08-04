from flask import Flask, request, jsonify
import pandas as pd
import json

import library.upload.upload as upload
import library.query.transactions as transaction_query

app = Flask(__name__)

# POST endpoint to handle file upload and data insertion
@app.route('/upload/file', methods=['POST'])
def upload_file():
    # Check if request contains a file part
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # Get file type and transaction type from request JSON body
    data = request.form.get('data')
    if not data:
        return jsonify({'error': 'No data part in the request'}), 400
    
    data = json.loads(data)
    file_type = data.get('file_type')
    transaction_type = data.get('transaction_type')
    user_id = data.get('user_id')
    
    if not file_type or not transaction_type:
        return jsonify({'error': 'Invalid file type or transaction type'}), 400
    if transaction_type not in ['credit', 'chequing']:
        return jsonify({'error': 'Invalid transaction type'}), 400

    # Process the data
    dataframe = pd.read_csv(file)
    if transaction_type == 'credit':
        result = upload.process_and_store_credit_dataframe(user_id, dataframe)
    elif transaction_type == 'chequing':
        result = upload.process_and_store_chequing_dataframe(user_id, dataframe)

    return jsonify(result)


# GET endpoint for querying credit transactions
@app.route('/credit_transactions/query/<user_id>', methods=['GET'])
def query_credit_transactions(user_id):
    filters = {}
    if request.args.get('start_date'): filters['start_date'] = request.args.get('start_date')
    if request.args.get('end_date'): filters['end_date'] = request.args.get('end_date')
    if request.args.get('description'): filters['description'] = request.args.get('description')
    if request.args.get('type'): filters['type'] = request.args.get('type')
    if request.args.get('amount_lt'): filters['amount_lt'] = request.args.get('amount_lt')
    if request.args.get('amount_eq'): filters['amount_eq'] = request.args.get('amount_eq')
    if request.args.get('amount_gt'): filters['amount_gt'] = request.args.get('amount_gt')
    if request.args.get('category'): filters['category'] = request.args.get('category')
    
    return jsonify(transaction_query.query(user_id=user_id, table_name='CreditTransactions', filters=filters))


# GET endpoint for querying chequing transactions
@app.route('/chequing_transactions/query/<user_id>', methods=['GET'])
def query_chequing_transactions(user_id):
    filters = {}
    if request.args.get('start_date'): filters['start_date'] = request.args.get('start_date')
    if request.args.get('end_date'): filters['end_date'] = request.args.get('end_date')
    if request.args.get('description'): filters['description'] = request.args.get('description')
    if request.args.get('type'): filters['type'] = request.args.get('type')
    if request.args.get('amount_lt'): filters['amount_lt'] = request.args.get('amount_lt')
    if request.args.get('amount_eq'): filters['amount_eq'] = request.args.get('amount_eq')
    if request.args.get('amount_gt'): filters['amount_gt'] = request.args.get('amount_gt')
    if request.args.get('balance_lt'): filters['balance_lt'] = request.args.get('balance_lt')
    if request.args.get('balance_eq'): filters['balance_eq'] = request.args.get('balance_eq')
    if request.args.get('balance_gt'): filters['balance_gt'] = request.args.get('balance_gt')
    if request.args.get('category'): filters['category'] = request.args.get('category')
    
    return jsonify(transaction_query.query(user_id=user_id, table_name='ChequingTransactions', filters=filters))

if __name__ == '__main__':
    app.run(port=8000)
