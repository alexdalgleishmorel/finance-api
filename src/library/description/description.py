from flask import jsonify
import pymysql
from constants import db_settings

def delete_description_mapping(original_description, user_id):
    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor() as cursor:
            # Delete the mapping where the OriginalDescription and UserID match
            cursor.execute(
                "DELETE FROM CustomTransactionDescriptionMapping WHERE OriginalDescription = %s AND UserID = %s", 
                (original_description, user_id)
            )
            connection.commit()
            # Check if the row was deleted
            if cursor.rowcount > 0:
                return jsonify({'message': 'Mapping deleted successfully.'}), 200
            else:
                return jsonify({'error': 'Mapping not found or not owned by this user.'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        connection.close()


def update_description_mapping(original_description, new_description, user_id):
    if not original_description or not user_id:
        return jsonify({'error': 'Original description and user ID are required.'}), 400
    if not new_description:
        return jsonify({'error': 'New custom description is required.'}), 400

    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor() as cursor:
            # Update the mapping where the OriginalDescription and UserID match
            sql = """
                UPDATE CustomTransactionDescriptionMapping 
                SET CustomDescription = %s 
                WHERE OriginalDescription = %s AND UserID = %s
            """
            cursor.execute(sql, (new_description, original_description, user_id))
            connection.commit()
            # Check if the row was updated
            if cursor.rowcount > 0:
                return jsonify({'message': 'Mapping updated successfully.'}), 200
            else:
                return jsonify({'error': 'Mapping not found or not owned by this user.'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        connection.close()
