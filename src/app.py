from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import json

import library.upload.upload as upload
import library.query.transactions as transaction_query
import library.category as category
import library.description as description

app = Flask(__name__)
CORS(app)


@app.route('/expenses/upload', methods=['POST'])
def upload_file():
    # Check if request contains a file part
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No filename'}), 400

    # Get file type and transaction type from request JSON body
    data = request.form.get('data')
    if not data:
        return jsonify({'error': 'No data part in the request'}), 400
    
    data = json.loads(data)
    file_type = data.get('file_type')
    user_id = data.get('user_id')
    
    if not file_type:
        return jsonify({'error': 'Invalid file type'}), 400

    # Process the data
    dataframe = pd.read_csv(file)
    result = upload.process_and_store_credit_dataframe(user_id, dataframe)

    return jsonify(result)


@app.route('/chequing/upload', methods=['POST'])
def upload_file():
    # Check if request contains a file part
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No filename'}), 400

    # Get file type and transaction type from request JSON body
    data = request.form.get('data')
    if not data:
        return jsonify({'error': 'No data part in the request'}), 400
    
    data = json.loads(data)
    file_type = data.get('file_type')
    user_id = data.get('user_id')
    
    if not file_type:
        return jsonify({'error': 'Invalid file type'}), 400

    # Process the data
    dataframe = pd.read_csv(file)
    result = upload.process_and_store_chequing_dataframe(user_id, dataframe)

    return jsonify(result)


# GET endpoint for querying credit transactions
@app.route('/expenses/query/<user_id>', methods=['GET'])
def query_credit_transactions(user_id):
    filters = {key: request.args.get(key) for key in transaction_query.GENERAL_FITLERS if request.args.get(key)}
    return jsonify(transaction_query.query(user_id=user_id, table_name='CreditTransactions', filters=filters))


# GET endpoint for querying chequing transactions
@app.route('/chequing/query/<user_id>', methods=['GET'])
def query_chequing_transactions(user_id):
    filters = {key: request.args.get(key) for key in transaction_query.GENERAL_FITLERS if request.args.get(key)}
    return jsonify(transaction_query.query(user_id=user_id, table_name='ChequingTransactions', filters=filters))


# Endpoint to delete a category by CategoryName for a specific user
@app.route('/expenses/category-mappings/delete', methods=['DELETE'])
def delete_category_mapping():
    data = request.json
    category_name = data.get('category_name')
    user_id = data.get('user_id')

    if not category_name or not user_id:
        return jsonify({'error': 'Category name and user ID are required.'}), 400

    return category.delete_category_mapping(category_name, user_id)


# Endpoint to update a category by CategoryName for a specific user
@app.route('/expenses/category-mappings/update', methods=['PUT'])
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
@app.route('/expenses/description-mappings/delete', methods=['DELETE'])
def delete_description_mapping():
    data = request.json
    original_description = data.get('original_description')
    user_id = data.get('user_id')

    if not original_description or not user_id:
        return jsonify({'error': 'Original description and user ID are required.'}), 400

    return description.delete_description_mapping(original_description, user_id)


# Endpoint to update a custom transaction description mapping by OriginalDescription for a specific user
@app.route('/expenses/description-mappings/update', methods=['PUT'])
def update_description_mapping():
    data = request.json
    original_description = data.get('original_description')
    user_id = data.get('user_id')
    new_description = data.get('new_description')

    return description.update_description_mapping(original_description, new_description, user_id)


if __name__ == '__main__':
    app.run(port=8000)
