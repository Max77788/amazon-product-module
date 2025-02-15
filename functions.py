import os
import time
import numpy as np
import requests
import json
import keepa
import random
from openai import OpenAI
from pymongo import MongoClient

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# -------------------------
# INITIALIZATION
# -------------------------
# Initialize OpenAI client (don’t hardcode keys in production!)
api_key = os.environ.get("MAX_OPENAI_KEY", os.environ.get("ANDIE_OPENAI_KEY"))
openai_client = OpenAI(api_key=api_key)
ASSISTANT_ID = os.environ.get("AMAZON_PRODUCT_ASSESSOR_ASSISTANT_ID")

# Initialize Keepa API
keepa_api = keepa.Keepa(os.environ.get("KEEPA_API_KEY"))

# Rainforest Collections API configuration
rainforest_api_key = os.environ.get('RAINFOREST_API_KEY')
RAINFOREST_COLLECTION_BASE_URL = f'https://api.rainforestapi.com/collections?api_key={rainforest_api_key}'
RAINFOREST_COLLECTION_BASE_URL_NO_API_KEY = 'https://api.rainforestapi.com/collections'

# MongoDB client
mongo_uri = os.environ.get("MONGO_URI")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["amazon-products"]
best_products_collection = db["assessments"]

# -------------------------
# HELPER FUNCTIONS
# -------------------------
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

# OpenAI product assessment helpers
def start_product_assessment(product_prompt):
    run = openai_client.beta.threads.create_and_run(
        assistant_id=ASSISTANT_ID,
        thread={"messages": [{"role": "user", "content": product_prompt}]}
    )
    return {"run_id": run.id, "thread_id": run.thread_id}

def retrieve_run_status(run_id, thread_id):
    run = openai_client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)
    run_status = run.status
    if run_status == "failed":
        return {"finished": True, "success": False, "message": "Run has failed", "status": run_status}
    if run_status == "completed":
        return {"finished": True, "success": True, "message": "Run has completed", "status": run_status}
    return {"finished": False, "success": False, "message": "Run is still running", "status": run_status}

def retrieve_product_assessment(thread_id):
    thread_messages = openai_client.beta.threads.messages.list(thread_id)
    response = thread_messages.data[0].content[0].text.value
    return response

def start_product_assessment_category(prompt):
    print("Sending category prompt to OpenAI...")
    run_info = start_product_assessment(prompt)
    run_id = run_info["run_id"]
    thread_id = run_info["thread_id"]
    for _ in range(30):
        status = retrieve_run_status(run_id, thread_id)
        if status["finished"]:
            break
        time.sleep(1)
    result_text = retrieve_product_assessment(thread_id)
    try:
        result_json = json.loads(result_text)
        print("Received ratings response:", result_json)
        return result_json  # Expecting {"ratings": [ ... ]}
    except Exception as e:
        print("Error parsing OpenAI response:", e)
        return {"ratings": []}

def generate_category_prompt(products, category_label):
    prompt = (
        f"You are an expert product evaluator. For the {category_label} category, "
        "here is a list of products with their details and reviews. "
        "For each product, return a JSON object with keys 'asin_code' and 'rating' (a number between 1 and 10) "
        "in a JSON object with the key 'ratings'. Format your output exactly as specified.\n\n"
    )
    for product in products:
        prompt += (
            f"Product ASIN: {product['asin']}\n"
            f"Price: ${product['price']}\n"
            f"Normalized Rating: {product['rating']}\n"
            f"Number of Reviews: {product['count_reviews']}\n"
            f"Reviews:\n{product.get('prompt', 'No reviews available')}\n\n"
        )
    prompt += "Return your output as a JSON object with a key 'ratings' which is a list of objects, each with 'asin_code' and 'rating'."
    print(f"Generated prompt for {category_label} with length {len(prompt)} characters")
    return prompt

# New helper: Enrich products using the Rainforest Collections API with "reviews_v2" requests.
def enrich_products_with_reviews(products, rainforest_api_key):
    if not products:
        return products

    # Get unique ASINs from our products list.
    asins = list({p['asin'] for p in products})
    print(f"Enriching {len(asins)} products using Rainforest Collections API (reviews_v2)...")

    # Step 1: Create a new collection
    collection_payload = {
        'name': 'Product Reviews Collection (reviews_v2)',
        'schedule_type': 'manual'
    }
    response = requests.post(RAINFOREST_COLLECTION_BASE_URL, json=collection_payload)
    collection = response.json()
    
    print(f"Collection Retrieved: {collection}")
    
    collection_id = collection['collection']['id']
    print(f"Created collection with id: {collection_id}")

    # Step 2: Add review requests to the collection using type "reviews_v2"
    requests_payload = []
    for asin in asins:
        requests_payload.append({
            'type': 'reviews_v2',
            'asin': asin,
            'amazon_domain': 'amazon.com'
        })
        
    body = {
        "requests": requests_payload
    }
            
    requests_url = f'{RAINFOREST_COLLECTION_BASE_URL_NO_API_KEY}/{collection_id}?api_key={rainforest_api_key}'
    
    response = requests.put(requests_url, json=body)
    print("Added review_v2 requests to collection.")

    # Step 3: Start the collection
    params = {
        'api_key': rainforest_api_key
    }
    start_url = f'{RAINFOREST_COLLECTION_BASE_URL_NO_API_KEY}/{collection_id}/start?api_key={rainforest_api_key}'
    response = requests.get(start_url)
    
    print(f"Response from starting the collection: {response.text}")
    
    print("Started collection for review requests.")

    # Step 4: Poll for results
    results_url = f'{RAINFOREST_COLLECTION_BASE_URL_NO_API_KEY}/{collection_id}/results'
    timeout = 120  # seconds
    poll_interval = 5
    elapsed = 0
    results = None
    while elapsed < timeout:
        response = requests.get(results_url, params)
        results = response.json()
        
        # print(f"Results of a collection pulling: {results}")
        
        if results.get("results"):
            if results["results"][0]["requests_completed"] == len(products):
                print("\n\n\n\nCompleted the product search\n\n\n\n")
                break
        time.sleep(poll_interval)
        elapsed += poll_interval
    else:
        print("Timeout reached while waiting for collection results.")

    # Build a mapping from asin to formatted review text.
    reviews_mapping = {}
    
    """
    for item in items if isinstance(items, list) else []:
        # Expecting the ASIN at the top level for reviews_v2.
        asin = item.get('asin')
        if not asin:
            continue
        # For reviews_v2, assume reviews are available in the "reviews" key.
        reviews = item.get('reviews', [])
        review_text = ""
        for review in reviews:
            review_text += (
                f"Title: {review.get('title', '')}\n"
                f"Rating: {review.get('rating', '')}\n"
                f"Review: {review.get('body', '').replace('Read more', '')}\n"
                f"---\n"
            )
        if not review_text:
            review_text = "No reviews available"
        reviews_mapping[asin] = review_text
    """

    # Update each product with its review text.
    for product in products:
        asin = product['asin']
        product['prompt'] = reviews_mapping.get(asin, "No reviews available")
        print(f"Product {asin} enriched; review prompt length: {len(product['prompt'])} characters")
    return products


def get_best_products(ratings):
    """
    For each category in the ratings dictionary, this function:
      1. Finds the highest rating.
      2. Filters products with that rating.
      3. Selects the cheapest product among them.
      4. If there's a tie, picks one randomly.
    Returns a dictionary mapping category names to the chosen product's asin_code.
    """
    best_products = {}

    for category, products in ratings.items():
        if not products:
            continue  # Skip empty category lists

        # 1. Get the highest rating in this category.
        highest_rating = max(product["rating"] for product in products)

        # 2. Filter products that have this highest rating.
        highest_rated = [p for p in products if p["rating"] == highest_rating]

        # 3. Among these, get the cheapest product.
        cheapest_price = min(p["price"] for p in highest_rated)
        cheapest_products = [p for p in highest_rated if p["price"] == cheapest_price]

        # 4. If there's a tie, choose one at random.
        chosen_product = random.choice(cheapest_products)
        best_products[category] = chosen_product["asin_code"]

    return best_products

# -------------------------
# MAIN PROCESS FUNCTION
# -------------------------
def process_assessment(unique_id, categories_ids):
    print("=== Starting process_assessment ===")
    print(f"Unique ID: {unique_id}")
    print(f"Categories IDs: {categories_ids}")

    # 1. Retrieve products from Keepa
    print("Step 1: Retrieving products from Keepa...")
    params = get_product_finder_params(categories_ids)
    print("Keepa parameters:", params)
    list_of_asins = keepa_api.product_finder(params)
    print(f"Total products found: {len(list_of_asins)}")
    top_asins = list_of_asins[:60]
    print("Top ASINs selected:", top_asins)
    products_details = keepa_api.query(top_asins, rating=True)
    print(f"Product details retrieved: {len(products_details)} products")

    # 2. Process product details from Keepa
    print("Step 2: Processing product details...")
    products_data = []
    prices = []
    for i, product in enumerate(products_details):
        try:
            asin = product['asin']
            price = product['csv'][0][-1] / 100  # Convert cents to dollars
            rating = product['csv'][16][-1] / 10   # Normalize rating
            count_reviews = product['csv'][17][-1]
            print(f"Product {i}: ASIN={asin}, Price=${price}, Rating={rating}, Reviews={count_reviews}")
        except Exception as e:
            print(f"Product {i}: Error processing product - {e}")
            continue
        products_data.append({
            "asin": asin,
            "price": price,
            "rating": rating,
            "count_reviews": count_reviews
        })
        prices.append(price)

    if not prices:
        print("No valid products found. Exiting process_assessment.")
        return {"error": "No valid products found."}

    prices_np = np.array(prices)
    percentiles = np.percentile(prices_np, [33, 66, 100])
    print("Calculated price percentiles:", percentiles)

    # 3. Partition products into low, mid, and high price segments
    print("Step 3: Partitioning products into price segments...")
    low_products, mid_products, high_products = [], [], []
    for product in products_data:
        if product["price"] <= percentiles[0]:
            low_products.append(product)
        elif product["price"] <= percentiles[1]:
            mid_products.append(product)
        else:
            high_products.append(product)
    
    print(f"Low price products count: {len(low_products)}")
    print(f"Mid price products count: {len(mid_products)}")
    print(f"High price products count: {len(high_products)}")

    # 4. Enrich all products with review data concurrently via Rainforest’s collections API (using reviews_v2)
    print("Step 4: Enriching products with reviews using Rainforest Collections API (reviews_v2)...")
    all_products = low_products + mid_products + high_products
    enrich_products_with_reviews(all_products, rainforest_api_key)

    # 5. Build prompts for each price segment and call OpenAI for product assessment
    print("Step 5: Generating prompts and retrieving ratings from OpenAI...")
    low_prompt = generate_category_prompt(low_products, "low price")
    mid_prompt = generate_category_prompt(mid_products, "mid price")
    high_prompt = generate_category_prompt(high_products, "high price")

    print("Sending low price prompt to OpenAI...")
    low_ratings_response = start_product_assessment_category(low_prompt)
    print("Sending mid price prompt to OpenAI...")
    mid_ratings_response = start_product_assessment_category(mid_prompt)
    print("Sending high price prompt to OpenAI...")
    high_ratings_response = start_product_assessment_category(high_prompt)

    ratings = {
        "low_price": low_ratings_response.get("ratings", []),
        "mid_price": mid_ratings_response.get("ratings", []),
        "high_price": high_ratings_response.get("ratings", [])
    }
    print("Ratings received:")
    print(ratings)

    # 6. Insert the ratings document into MongoDB
    print("Step 6: Inserting document into MongoDB...")
    document = {
        "unique_id": unique_id,
        "ratings": ratings
    }
    best_products_collection.insert_one(document)
    print("Document inserted into MongoDB:", document)

    print("Step 7: Process completed. Returning ratings:")
    print(ratings)
    return ratings