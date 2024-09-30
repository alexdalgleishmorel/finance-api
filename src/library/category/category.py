from flask import jsonify
import pymysql

from constants import db_settings

def delete_category_mapping(category_name, user_id):
    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor() as cursor:
            # Delete the category where the CategoryName and UserID match
            cursor.execute(
                "DELETE FROM UserCategories WHERE CategoryName = %s AND UserID = %s", 
                (category_name, user_id)
            )
            connection.commit()
            # Check if the row was deleted
            if cursor.rowcount > 0:
                return jsonify({'message': 'Category deleted successfully.'}), 200
            else:
                return jsonify({'error': 'Category not found or not owned by this user.'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        connection.close()


def update_category_mapping(user_id, category_name, new_category_name, description):
    update_fields = []
    params = []
    if new_category_name:
        update_fields.append("CategoryName = %s")
        params.append(new_category_name)
    if description:
        update_fields.append("Description = %s")
        params.append(description)

    # Add CategoryName and UserID to the params list for the WHERE clause
    params.append(category_name)
    params.append(user_id)

    connection = pymysql.connect(**db_settings)
    try:
        with connection.cursor() as cursor:
            # Update the category where the CategoryName and UserID match
            sql = f"UPDATE UserCategories SET {', '.join(update_fields)} WHERE CategoryName = %s AND UserID = %s"
            cursor.execute(sql, params)
            connection.commit()
            # Check if the row was updated
            if cursor.rowcount > 0:
                return jsonify({'message': 'Category updated successfully.'}), 200
            else:
                return jsonify({'error': 'Category not found or not owned by this user.'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        connection.close()
