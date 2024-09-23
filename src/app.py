from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import json

import library.upload.upload as upload
import library.query.transactions as transaction_query
import category

app = Flask(__name__)
CORS(app)

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
    
    return jsonify(transaction_query.query(user_id=user_id, table_name='Transactions', filters=filters))


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
    
    return jsonify(transaction_query.query(user_id=user_id, table_name='Transactions', filters=filters))


# Endpoint to delete a category by CategoryName for a specific user
@app.route('/credit_transactions/category-mappings/delete', methods=['DELETE'])
def delete_category_mapping():
    data = request.json
    category_name = data.get('category_name')
    user_id = data.get('user_id')

    if not category_name or not user_id:
        return jsonify({'error': 'Category name and user ID are required.'}), 400

    return category.delete_category_mapping(category_name, user_id)


# Endpoint to update a category by CategoryName for a specific user
@app.route('/credit_transactions/category-mappings/update', methods=['PUT'])
def update_category_mapping():
    data = request.json
    category_name = data.get('category_name')
    user_id = data.get('user_id')
    new_category_name = data.get('new_category_name')
    description = data.get('description')

    if not category_name or not user_id:
        return jsonify({'error': 'Category name and user ID are required.'}), 400
    if not new_category_name and not description:
        return jsonify({'error': 'No update data provided.'}), 400

    return category.update_category_mapping(user_id, category_name, new_category_name, description)


# Endpoint to delete a custom transaction description mapping by OriginalDescription for a specific user
@app.route('/credit_transactions/description-mappings/delete', methods=['DELETE'])
def delete_description_mapping():
    data = request.json
    original_description = data.get('original_description')
    user_id = data.get('user_id')

    if not original_description or not user_id:
        return jsonify({'error': 'Original description and user ID are required.'}), 400

    return description.delete_description_mapping(original_description, user_id)


# Endpoint to update a custom transaction description mapping by OriginalDescription for a specific user
@app.route('/credit_transactions/description-mappings/update', methods=['PUT'])
def update_description_mapping():
    data = request.json
    original_description = data.get('original_description')
    user_id = data.get('user_id')
    new_custom_description = data.get('new_custom_description')

    return description.update_description_mapping(original_description, new_description, user_id)


if __name__ == '__main__':
    app.run(port=8000)
