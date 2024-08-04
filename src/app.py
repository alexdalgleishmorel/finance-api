from flask import Flask, request, jsonify
import pandas as pd
import json

import library.upload.upload as upload

app = Flask(__name__)

# Route to handle file upload and data insertion
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

    return jsonify({'message': json.dumps(result)})

if __name__ == '__main__':
    app.run(port=8000)
