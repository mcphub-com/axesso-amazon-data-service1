import requests
from datetime import datetime
from typing import Union, Literal, List
from mcp.server import FastMCP
from pydantic import Field
from typing import Annotated
from mcp.server.fastmcp import FastMCP
from fastmcp import FastMCP, Context
import os
from dotenv import load_dotenv
load_dotenv()
rapid_api_key = os.getenv("RAPID_API_KEY")
import asyncio
import httpx
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

__rapidapi_url__ = 'https://rapidapi.com/axesso/api/axesso-amazon-data-service1'

mcp = FastMCP('axesso-amazon-data-service1')

@mcp.tool()
def product_details(url: Annotated[str, Field(description='Amazon product URL to fetch details for.')],
                    merchant: Annotated[Union[str, None], Field(description='merchant id')] = None) -> dict: 
    '''Retrieve product details from Amazon using the product URL.'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-lookup-product'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'url': url,
        'merchant': merchant,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def search_products(domainCode: Annotated[str, Field(description='Amazon domain suffix (e.g., com, de, fr, co.uk).')],
                    keyword: Annotated[str, Field(description='keyword used for the search')],
                    page: Annotated[str, Field(description='Parameter for Pagination')],
                    excludeSponsored: Annotated[Union[str, None], Field(description='Set true to exclude sponsored products')] = None,
                    sortBy: Annotated[Union[str, None], Field(description='sorting for the search results')] = None,
                    withCache: Annotated[Union[bool, None], Field(description='If true, proxy for same searchterm will be cached and used for next page. Might increase the response quality.')] = None,
                    category: Annotated[Union[str, None], Field(description='Pass the amazon category which should used for the search. Valid category list can found on the amazon website on the search selection box. Important: If the passed category is not a valid amazon category, the response will be empty')] = None,
                    browseNode: Annotated[Union[str, None], Field(description='Define the current browsing node (category or sub-category). E.g. pass 10158976011 to get Amazon Resale')] = None,
                    nodeHierarchy: Annotated[Union[str, None], Field(description='Further category nodes hierarchy. Can be a single category number or a comma separated list, e.g. 3375251,10158976011,706814011,11051400011')] = None) -> dict: 
    '''Search for products on Amazon by keyword and return ASINs. Supports filtering by category, sort order, and more.'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-search-by-keyword-asin'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'domainCode': domainCode,
        'keyword': keyword,
        'page': page,
        'excludeSponsored': excludeSponsored,
        'sortBy': sortBy,
        'withCache': withCache,
        'category': category,
        'browseNode': browseNode,
        'nodeHierarchy': nodeHierarchy,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()
    
@mcp.tool()
async def get_full_product_list(
    domainCode: Annotated[str, Field(description='e.g. com / de / fr / co.uk')],
    keyword: Annotated[str, Field(description='keyword used for the search')],
    excludeSponsored: Annotated[Union[str, None], Field(description='Set true to exclude sponsored products')] = None,
    sortBy: Annotated[Union[str, None], Field(description='sorting for the search results')] = None,
    withCache: Annotated[Union[bool, None], Field(description='If true, proxy for same searchterm will be cached and used for next page. Might increase the response quality.')] = None,
    category: Annotated[Union[str, None], Field(description='Pass the amazon category which should used for the search. Valid category list can found on the amazon website on the search selection box. Important: If the passed category is not a valid amazon category, the response will be empty')] = None,
    browseNode: Annotated[Union[str, None], Field(description='Define the current browsing node (category or sub-category). E.g. pass 10158976011 to get Amazon Resale')] = None,
    nodeHierarchy: Annotated[Union[str, None], Field(description='Further category nodes hierarchy. Can be a single category number or a comma separated list, e.g. 3375251,10158976011,706814011,11051400011')] = None
) -> dict:
    """Fetch the complete product list: fetch 13 pages in batches of 3, with retry and delay to avoid rate limiting. Merge all product results."""
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-search-by-keyword-asin'
    headers = {
        'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com',
        'x-rapidapi-key': rapid_api_key
    }

    async def fetch_page_with_retry(page, max_retries=3):
        params = {
            'domainCode': domainCode,
            'keyword': keyword,
            'page': str(page),
            'excludeSponsored': excludeSponsored,
            'sortBy': sortBy,
            'withCache': withCache,
            'category': category,
            'browseNode': browseNode,
            'nodeHierarchy': nodeHierarchy,
        }
        params = {k: v for k, v in params.items() if v is not None}
        attempt = 0
        while attempt < max_retries:
            logger.info(f"Requesting page {page} with params: {params} (attempt {attempt+1})")
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    resp = await client.get(url, headers=headers, params=params)
                    if resp.status_code == 429:
                        logger.info(f"Page {page} got 429 Too Many Requests. Retrying after delay...")
                        attempt += 1
                        await asyncio.sleep(2)
                        continue
                    result = resp.json()
                    # Log the number of items for this page (merged field)
                    if isinstance(result, dict):
                        items = (
                            result.get('searchProductDetails')
                            or result.get('searchResults')
                            or result.get('products')
                            or result.get('items')
                            or []
                        )
                        logger.info(f"Page {page} returned {len(items)} items.")
                    else:
                        logger.info(f"Page {page} returned non-dict result.")
                    return result
            except httpx.ReadTimeout:
                logger.info(f"Page {page} timed out. Retrying after delay...")
                attempt += 1
                await asyncio.sleep(2)
            except Exception as e:
                logger.info(f"Page {page} encountered error: {e}. Retrying after delay...")
                attempt += 1
                await asyncio.sleep(2)
        logger.info(f"Page {page} failed after {max_retries} attempts. Returning empty result.")
        return {}

    async def gather_all():
        all_items = []
        batch_size =5
        total_pages = 13
        for batch_start in range(1, total_pages + 1, batch_size):
            batch = [fetch_page_with_retry(page) for page in range(batch_start, min(batch_start + batch_size, total_pages + 1))]
            results = await asyncio.gather(*batch)
            for result in results:
                if isinstance(result, dict):
                    items = (
                        result.get('searchProductDetails')
                        or result.get('searchResults')
                        or result.get('products')
                        or result.get('items')
                        or []
                    )
                    all_items.extend(items)
            await asyncio.sleep(1)  # Delay between batches to avoid rate limiting
        return {'all_items': all_items}

    return await gather_all()

@mcp.tool()
def reviews(domainCode: Annotated[str, Field(description='Amazon domain suffix (e.g., com, de, fr, co.uk).')],
            asin: Annotated[str, Field(description='asin used for the lookup')],
            sortBy: Annotated[Union[str, None], Field(description='Sorting of review results')] = None,
            filters: Annotated[Union[str, None], Field(description='Optional semicolon-separated list of review filters.')] = None) -> dict:
    '''Retrieve product reviews from Amazon using ASIN.'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-lookup-reviews'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'domainCode': domainCode,
        'asin': asin,
        'sortBy': sortBy,
        'filters': filters,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def deals(domainCode: Annotated[str, Field(description='all available marketplaces like com, de, co.uk, fr, it and other')],
          page: Annotated[Union[int, float], Field(description='Use this parameter to request deals page by page. Each page includes up to 30 deals Default: 1')],
          departments: Annotated[Union[str, None], Field(description="Returns deals within this provided department. Department id can be either fetched from amazon page via browser or using our API endpoint 'dealFilter'")] = None,
          discountRange: Annotated[Union[str, None], Field(description="Returns deals within a this discount range. Find an overview of the possible values in our endpoint 'dealFilter' Valid values are 1, 2, 3, 4,5")] = None,
          minProductStarRating: Annotated[Union[str, None], Field(description="Returns deals within a this review range. Find an overview of the possible values in our endpoint 'dealFilter' Valid values are 1, 2, 3, 4")] = None,
          priceRange: Annotated[Union[str, None], Field(description="Returns deals within a this price range. Find an overview of the possible values in our endpoint 'dealFilter' Valid values are 1, 2, 3, 4, 5")] = None,
          primeEarly: Annotated[Union[bool, None], Field(description='Returns prime early deals only')] = None,
          primeExclusive: Annotated[Union[bool, None], Field(description='Returns prime exclusive deals only')] = None) -> dict: 
    '''Lookup current deals available on Amazon (Today's Deals / Top Deals, Best Deals, and Lightning Deals) for different marketplaces using various filter options like department, price and discount.'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/v2/amazon-search-deals'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'domainCode': domainCode,
        'page': page,
        'departments': departments,
        'discountRange': discountRange,
        'minProductStarRating': minProductStarRating,
        'priceRange': priceRange,
        'primeEarly': primeEarly,
        'primeExclusive': primeExclusive,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def deals_filter(domainCode: Annotated[str, Field(description='All available marketplaces like com, de, co.uk, fr, it and other')]) -> dict: 
    '''Filter options which can be used in 'deals' endpoint'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/v2/amazon-deal-filter'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'domainCode': domainCode,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def offers(page: Annotated[str, Field(description='Number of pages to return')],
           domainCode: Annotated[str, Field(description='e.g. com / de / fr / co.uk')],
           asin: Annotated[str, Field(description='asin to lookup')]) -> dict: 
    '''Lookup offer on Amazon from different seller for the specified product'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/v2/amz/amazon-lookup-prices'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'page': page,
        'domainCode': domainCode,
        'asin': asin,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def best_seller(url: Annotated[Union[str, None], Field(description='url used for the lookup')] = None,
                page: Annotated[Union[str, None], Field(description='Parameter used for pagination')] = None) -> dict: 
    '''Lookup best seller list on Amazon by providing url and page.'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-best-sellers-list'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'url': url,
        'page': page,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def seller_details(sellerId: Annotated[Union[str, None], Field(description='id of the seller used for the lookup')] = None,
                   domainCode: Annotated[Union[str, None], Field(description='Domain code of the amazon page')] = None) -> dict: 
    '''Lookup seller details on Amazon based on sellerid and domain'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-lookup-seller'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'sellerId': sellerId,
        'domainCode': domainCode,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def seller_products(domainCode: Annotated[str, Field(description='domain code of the amazon page')],
                    sellerId: Annotated[str, Field(description='seller id used for the lookup')],
                    page: Annotated[Union[int, float], Field(description='Parameter used for pagination Default: 1')]) -> dict: 
    '''Lookup all products on Amazon for a given seller page by page.'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-seller-products'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'domainCode': domainCode,
        'sellerId': sellerId,
        'page': page,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def profile(path: Annotated[str, Field(description='Path to the user profile. Can be obtained from "Reviews" endpoint, e.g. /gp/profile/amzn1.account.AFZFUKCD5Z5RKCEO3E7ODCL2SURQ/ref=cm_cr_dp_d_gw_tr?ie=UTF8')],
            domainCode: Annotated[str, Field(description='Supports nearly all amazon marketplaces like com / de / fr / co.uk / it / es /ca / co.uk etc.')]) -> dict: 
    '''Retrieve detailed profile information on Amazon, including user reviews and other relevant data.'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-lookup-profile'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'path': path,
        'domainCode': domainCode,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def healthcheck() -> dict: 
    '''test'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/actuator/health'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

@mcp.tool()
def review_details(reviewId: Annotated[str, Field(description='')],
                   domainCode: Annotated[str, Field(description='')]) -> dict: 
    '''Retrieve in-depth review details from Amazon, including ratings, review text, author information, and timestamps.'''
    url = 'https://axesso-axesso-amazon-data-service-v1.p.rapidapi.com/amz/amazon-review-details'
    headers = {'x-rapidapi-host': 'axesso-axesso-amazon-data-service-v1.p.rapidapi.com', 'x-rapidapi-key': rapid_api_key}
    payload = {
        'reviewId': reviewId,
        'domainCode': domainCode,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = requests.get(url, headers=headers, params=payload)
    return response.json()



if __name__ == '__main__':
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9997
    mcp.run(transport="stdio")
