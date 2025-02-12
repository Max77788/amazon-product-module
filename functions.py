import os
import asyncio
import numpy as np
import requests
import json
import keepa
from openai import OpenAI
from pymongo import MongoClient

# Initialize OpenAI client
openai_client = OpenAI(api_key=os.environ.get("ANDIE_OPENAI_ACC", ""))
ASSISTANT_ID = os.environ.get("AMAZON_PRODUCT_ASSESSOR_ASSISTANT_ID")

# Initialize Keepa API
keepa_api = keepa.Keepa(os.environ.get("KEEPA_API_KEY"))

# Rainforest API configuration
RAINFOREST_URL = "https://api.rainforestapi.com/request"
rainforest_api_key = os.environ.get('RAINFOREST_API_KEY')

# MongoDB client (for inserting the best product results)
mongo_uri = os.environ.get("MONGO_URI")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["amazon-products"]
best_products_collection = db["assessments"]

# Price categories mapping (for labeling)
price_cats = {
    0: "low_price",
    1: "mid_price",
    2: "high_price"
}

# Helper: Build parameters for Keepa product finder
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

# Helper: Build parameters for Rainforest API reviews request
def get_params_for_reviews_req(asin):
    return {
        'api_key': rainforest_api_key,
        'amazon_domain': 'amazon.com',
        'asin': asin,
        'type': 'product'
    }

# -------------------------
# OpenAI Product Assessment
# -------------------------
def start_product_assessment(product_prompt):
    run = openai_client.beta.threads.create_and_run(
        assistant_id=ASSISTANT_ID,
        thread={
            "messages": [
                {"role": "user", "content": product_prompt}
            ]
        }
    )
    return {"run_id": run.id, "thread_id": run.thread_id}

def retrieve_run_status(run_id, thread_id):
    run = openai_client.beta.threads.runs.retrieve(
        thread_id=thread_id,
        run_id=run_id
    )
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

# Async wrappers
async def async_start_product_assessment(product_prompt):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, start_product_assessment, product_prompt)

async def async_retrieve_run_status(run_id, thread_id):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, retrieve_run_status, run_id, thread_id)

async def async_retrieve_product_assessment(thread_id):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, retrieve_product_assessment, thread_id)

# Helper function: For a given product, start an assessment run and return its rating.
async def async_get_rating(product):
    """
    Expects a product dict with keys "asin" and "prompt" (the review text).
    Constructs a prompt that instructs the assistant to rate the product based on reviews.
    """
    prompt = (
        f"Rate the product with ASIN {product['asin']} based on these reviews:\n"
        f"{product['prompt']}\n"
        "Return a JSON object with keys 'asin_code' and 'rating' (number between 1 and 10)."
    )
    run_info = await async_start_product_assessment(prompt)
    run_id = run_info["run_id"]
    thread_id = run_info["thread_id"]
    
    # Poll until the run is complete (timeout after ~30 seconds)
    for _ in range(30):
        status = await async_retrieve_run_status(run_id, thread_id)
        if status["finished"]:
            break
        await asyncio.sleep(1)
    # Retrieve the result
    result_text = await async_retrieve_product_assessment(thread_id)
    try:
        result_json = json.loads(result_text)
        return result_json  # Expected to contain {"asin_code": ..., "rating": ...}
    except Exception:
        return {"asin_code": product["asin"], "rating": 0}

# -------------------------
# Main Process Function
# -------------------------
async def process_assessment(unique_id, categories_ids):
    """
    1. Finds products using Keepa based on the provided category IDs.
    2. Retrieves product details and computes price percentiles.
    3. Partitions products into low, mid, and high price segments.
    4. For each product, retrieves review data from Rainforest API and builds a review prompt.
    5. Concurrently gets a product rating via the OpenAI assistant.
    6. Chooses the best product (highest rating) in each segment.
    7. Inserts a document into MongoDB (with the provided unique_id) containing the best products.
    8. Returns the best products dictionary.
    """
    print("=== Starting process_assessment ===")
    print(f"Unique ID: {unique_id}")
    print(f"Categories IDs: {categories_ids}")
    
    # 1. Get products from Keepa
    print("Step 1: Retrieving products from Keepa...")
    params = get_product_finder_params(categories_ids)
    print("Keepa parameters:", params)
    list_of_asins = keepa_api.product_finder(params)
    print(f"Total products found: {len(list_of_asins)}")
    top_asins = list_of_asins[:60]
    print("Top ASINs selected:", top_asins)
    products_details = keepa_api.query(top_asins, rating=True)
    print(f"Product details retrieved: {len(products_details)} products")
    
    # 2. Process product details
    print("Step 2: Processing product details...")
    products_data = []
    prices = []
    for i, product in enumerate(products_details):
        try:
            asin = product['asin']
            price = product['csv'][0][-1] / 100  # Convert to dollars
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
    
    # 3. Partition products by price segments
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
    
    # 4. Enrich each product with review text from Rainforest API.
    print("Step 4: Enriching products with reviews from Rainforest API...")

    async def enrich_product_with_reviews(product):
        print(f"Enriching product {product['asin']}...")
        params = get_params_for_reviews_req(product["asin"])
        response = requests.get(RAINFOREST_URL, params=params).json()
        reviews = response.get('product', {}).get('top_reviews', [])
        reviews_text = ""
        for review in reviews:
            reviews_text += (
                f"Title: {review.get('title', '')}\n"
                f"Rating: {review.get('rating', '')}\n"
                f"Review: {review.get('body', '').replace('Read more', '')}\n"
                f"---\n"
            )
        product["prompt"] = reviews_text
        print(f"Product {product['asin']} enriched. Review prompt length: {len(reviews_text)} characters")
        return product

    print("Enriching low price products...")
    low_products = await asyncio.gather(*[asyncio.to_thread(enrich_product_with_reviews, p) for p in low_products])
    print("Enriching mid price products...")
    mid_products = await asyncio.gather(*[asyncio.to_thread(enrich_product_with_reviews, p) for p in mid_products])
    print("Enriching high price products...")
    high_products = await asyncio.gather(*[asyncio.to_thread(enrich_product_with_reviews, p) for p in high_products])
    
    # 5. Concurrently get ratings using async_get_rating for each product.
    print("Step 5: Getting product ratings concurrently via OpenAI assistant...")
    low_tasks = [async_get_rating(p) for p in low_products]
    mid_tasks = [async_get_rating(p) for p in mid_products]
    high_tasks = [async_get_rating(p) for p in high_products]
    
    print("Executing rating tasks for low price products...")
    low_ratings = await asyncio.gather(*low_tasks)
    print("Low price ratings:", low_ratings)
    
    print("Executing rating tasks for mid price products...")
    mid_ratings = await asyncio.gather(*mid_tasks)
    print("Mid price ratings:", mid_ratings)
    
    print("Executing rating tasks for high price products...")
    high_ratings = await asyncio.gather(*high_tasks)
    print("High price ratings:", high_ratings)
    
    # 6. Select the best product per segment based on the highest rating.
    print("Step 6: Selecting best product in each segment...")
    def select_best(ratings_list):
        if not ratings_list:
            return None
        best = max(ratings_list, key=lambda r: r.get("rating", 0))
        return best.get("asin_code")
    
    best_low = select_best(low_ratings)
    best_mid = select_best(mid_ratings)
    best_high = select_best(high_ratings)
    
    print("Best low price product ASIN:", best_low)
    print("Best mid price product ASIN:", best_mid)
    print("Best high price product ASIN:", best_high)
    
    best_products = {
        "low_price": best_low,
        "mid_price": best_mid,
        "high_price": best_high
    }
    
    # 7. Insert the results into MongoDB using the provided unique_id.
    print("Step 7: Inserting best products document into MongoDB...")
    document = {
        "unique_id": unique_id,
        "best_products": best_products
    }
    best_products_collection.insert_one(document)
    print("Document inserted into MongoDB:", document)
    
    # 8. Return the best products dictionary.
    print("Step 8: Process completed. Returning best products:")
    print(best_products)
    return best_products
