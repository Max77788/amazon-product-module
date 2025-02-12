from flask import Flask, request, jsonify
from functions import process_assessment
from pymongo import MongoClient
import os
import asyncio
# from uuid import uuid4
import shortuuid

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

app = Flask(__name__)

# Setup MongoDB client
mongo_uri = os.environ.get("MONGO_URI")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["amazon-products"]
best_products_collection = db["assessments"]

@app.route('/start_assessment', methods=['POST'])
async def start_assessment():
    data = request.get_json()
    print("Data: ", data)
    unique_id = shortuuid.ShortUUID().random(length=8)
    categories_ids = data.get("categories_ids")
    
    if not categories_ids:
        return jsonify({"error": "unique_id and categories_ids are required"}), 400

   # Trigger process_assessment as a background task without waiting for it to complete.
    asyncio.create_task(process_assessment(unique_id, categories_ids))
    
    return jsonify({"message": "Assessment started", "unique_id": unique_id})

@app.route('/check_result', methods=['POST'])
def check_result():
    data = request.get_json()
    unique_id = data.get("unique_id")
    if not unique_id:
        return jsonify({"error": "unique_id is required"}), 400
    
    # Query MongoDB for the result using the provided unique_id
    result = best_products_collection.find_one({"unique_id": unique_id})
    if result:
        return jsonify({"best_products": result.get("best_products"), "success": True})
    else:
        return jsonify({"message": "Result not found", "success": False}), 404


@app.route('/async')
async def async_route():
    # Your async code here
    return 'This is an async response!'


@app.route('/', methods=['GET'])
def index():
    return "Hi, there!!!"

if __name__ == '__main__':
    # For proper async support in production, consider running with an ASGI server (like Uvicorn)
    app.run(debug=os.environ.get("NODE_ENV", "none") == "development")
