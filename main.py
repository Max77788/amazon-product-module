from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv, find_dotenv
import os
import keepa
import numpy as np
import requests

load_dotenv(find_dotenv())

from openai import OpenAI
client = OpenAI(api_key=os.environ.get("ANDIE_OPENAI_ACC", ""))

import json

FLASK_DEBUG = os.environ.get('FLASK_DEBUG', "false").lower() == "true"
RAINFOREST_URL = "https://api.rainforestapi.com/request"

keepa_api = keepa.Keepa(os.environ.get('KEEPA_API_KEY'))
rainforest_api_key = os.environ.get('RAINFOREST_API_KEY')

price_cats = {
    0: "cheap",
    1: "medium",
    2: "expensive"
}

product_rating_schema = {
  "name": "product_rating",
  "schema": {
    "type": "object",
    "properties": {
      "asin_code": {
        "type": "string",
        "description": "The Amazon Standard Identification Number (ASIN) of the product."
      },
      "rating": {
        "type": "number",
        "description": "The rating of the product, must be between 1 and 10."
      }
    },
    "required": [
      "asin_code",
      "rating"
    ],
    "additionalProperties": False
  },
  "strict": True
}

def get_product_finder_params(category_ids):
    return {
  "page": 0,
  "per_page": 100,
  "root_category": None,
  "categories_include": category_ids,
  "productType": 0,
  "availabilityAmazon": 0,
  "hasReviews": True,
  "current_RATING_gte": 40,
  "current_COUNT_REVIEWS_gte": 20,
  "sort": ["avg180_RATING", "desc"]
}

def get_params_for_reviews_req(asin):
    return {
  'api_key': rainforest_api_key,
  'amazon_domain': 'amazon.com',
  'asin': asin,
  'type': 'product'
}

app = Flask(__name__)

@app.route('/api', methods=['POST'])
def api():
    data = request.get_json()
    return jsonify(data)

@app.route('/api/categories-to-three-best-products/get-batch-ids', methods=['POST'])
def categories_to_three_best_products_get_batch_ids():
    print("Received POST request to /api/categories-to-three-best-products")
    data = request.get_json()
    print(f"Received data: {data}")

    categories = data["categories_ids"]
    print(f"Categories to search: {categories}")

    params = get_product_finder_params(categories)
    print(f"Search parameters: {params}")

    # Find 100 best in these categories being 4 stars+ and 20+ reviews
    print("Searching for products...")
    list_of_asins = keepa_api.product_finder(params)
    print(f"Found {len(list_of_asins)} products")

    top100_asins = list_of_asins[:60]
    print(f"Taking top 100 products: {top100_asins}")
    
    # Obtain the price, average rating, and number of reviews for each one
    print("Querying detailed product information...")
    products_top100 = keepa_api.query(top100_asins, rating=True)
    print(f"Retrieved details for {len(products_top100)} products")

    products_data = []
    prices = []
    
    print("Processing product details...")
    for i, product in enumerate(products_top100):
        product_dict = {
            "asin": product['asin'],
            "price": product['csv'][0][-1]/100,
            "rating": product['csv'][16][-1]/10,
            "count_reviews": product['csv'][17][-1]
        }

        prices.append(product_dict["price"])

        # print(f"Product {i+1}: {product_dict}")
        products_data.append(product_dict)
    
    prices_np = np.array(prices)

    percentiles = np.percentile(prices_np, [33, 66, 100], method='linear')

    print(f"Prices percentiles: {percentiles}")
    
    low_price_products = []
    medium_price_products = []
    high_price_products = []

    for product in products_data:
        # Obtain written reviews for each product
        params = get_params_for_reviews_req(product["asin"])
        
        reviews_string = ""
        
        response_raw = requests.get(RAINFOREST_URL, params).json()
        
        reviews_raw = response_raw.get('product', {}).get('top_reviews', [])
        
        for review in reviews_raw:
            review_to_add = f"""
            Title: {review["title"]}
            Rating: {review["rating"]}
            Review: {review["body"].replace("Read more", "")}
            Number of Reviews: {product["count_reviews"]}
            
            ---
            """
            reviews_string += review_to_add
        
        product["review_description"] = reviews_string
        
        print(f"Reviews string: {reviews_string}")
        
        if product["price"] <= percentiles[0]:
            low_price_products.append(product)
        elif product["price"] <= percentiles[1]:
            medium_price_products.append(product)
        else:
            high_price_products.append(product)
    
    # Form the requests to Batch API
    three_lists = [low_price_products, medium_price_products, high_price_products]
    
    instruction_to_the_model = """
    Return the total score of the product on the scale from 1 to 10.
    Here is the details of the product and its reviews:
    """
    
    batch_ids_list = {}
    
    for list_index, list_of_products in enumerate(three_lists):
        requests_list = []
        for product_index, product in enumerate(list_of_products):
            product_prompt = f"""
            {instruction_to_the_model}
            
            {product["review_description"]}
            """
            
            dict_obj = {}
            dict_obj["custom_id"] = f"request-{price_cats[list_index]}-{product_index}"
            dict_obj["method"] = "POST"
            dict_obj["url"] = "/v1/chat/completions"
            dict_obj["body"] = {
                "model": "gpt-4o-mini",
                "messages": [{"role": "system", "content": "You are the assessor of Amazon products."},{"role": "user", "content": product_prompt}],
                "max_tokens": 1500,
                "response_format": product_rating_schema
            }
            requests_list.append(dict_obj)
            
        jsonl_file_name = f"batchinput_{list_index}.jsonl"
        
        # Open the file in write mode
        with open(jsonl_file_name, "w", encoding="utf-8") as file:
            for entry in data:
                file.write(json.dumps(entry) + "\n")
        
        batch_input_file = client.files.create(
            file=open(jsonl_file_name, "rb"),
            purpose="batch"
        )
        
        batch_input_file_id = batch_input_file.id

        batch = client.batches.create(
            input_file_id=batch_input_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={
                "description": "amazon products assessment"
            }
        )
        
        batch_id = batch.id
        
        batch_ids_list[price_cats[list_index]] = batch_id
            
    # Pass the data to AI and ask to return winner in each rank
    # print("Returning placeholder results")
    # three_best_asins = ["RDASFGWAFW", "ASDFASDFASDF", "ASDFASDFASDF"]
    
    return jsonify(batch_ids_list)

@app.route('/api/categories-to-three-best-products/check-batch-id-completion', methods=['POST'])
def categories_to_three_best_products_check_batch_id_completion():
    data = request.get_json()
    
    batch_id = data["batch_id"]
    
    batch = client.batches.retrieve(batch_id)
    
    if batch.status != "completed":
       return jsonify({"message": "Batch is not completed yet!", "success": False, "result_file_id": None })
    else:
        return jsonify({"message": "Batch is completed!", "success": True, "result_file_id": batch.output_file_id })
    
    
@app.route('/api/categories-to-three-best-products/retrieve-winner', methods=['POST'])
def categories_to_three_best_products_retrieve_winner():    
    data = request.get_json()
    
    output_file_id = data["result_file_id"]
    
    file_response = client.files.content(output_file_id)
    
    json_response = json.loads(file_response.text)
    
    # Obtain the ratings produced by the AI model
    
@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=FLASK_DEBUG)