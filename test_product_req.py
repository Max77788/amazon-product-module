import keepa
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os 
import requests

RAINFOREST_URL = "https://api.rainforestapi.com/request"

rainforest_api_key = os.environ.get('RAINFOREST_API_KEY')

def get_params_for_reviews_req(asin):
    return {
  'api_key': rainforest_api_key,
  'amazon_domain': 'amazon.com',
  'asin': asin,
  'type': 'product'
}

keepa_api = keepa.Keepa(os.environ.get('KEEPA_API_KEY'))

#products_top100 = keepa_api.query(["B0DJ2MY7ZG", "B0C7JNJW2N"], rating=True)

#with open('test_product_req.json', 'w') as f:
    #f.write(str(products_top100))

reviews_raw = requests.get(RAINFOREST_URL, get_params_for_reviews_req("B0DJ2MY7ZG"))
        
print(reviews_raw.json())