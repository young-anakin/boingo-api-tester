from fastapi import APIRouter, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials
import httpx
import json
from datetime import datetime
from ..models.models import (
    ScrapingResultCreate, ScrapingResultUpdate,
    ScrapingResultDelete
)
from ..core.config import BOINGO_API_URL
from .auth import security

router = APIRouter(
    prefix="/scraping-results",
    tags=["Scraping Result API"],
    responses={404: {"description": "Not found"}},
)

@router.get("")
async def get_all_results(credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Get all scraping results
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{BOINGO_API_URL}/scraping-results",
                headers={"Authorization": f"Bearer {credentials.credentials}"}
            )
            # Accept all 2xx status codes as success
            if response.status_code < 200 or response.status_code >= 300:
                raise HTTPException(status_code=response.status_code, detail=response.text)
            return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting results: {str(e)}")

@router.get("/{result_id}")
async def get_result_by_id(result_id: str, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Get scraping result by ID
    """
    try:
        print(f"\n=== Get Result Request ===")
        print(f"Result ID: {result_id}")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{BOINGO_API_URL}/scraping-results/{result_id}",
                    headers={"Authorization": f"Bearer {credentials.credentials}"}
                )
                
                print(f"\n=== Get Result Response ===")
                print(f"Status code: {response.status_code}")
                print(f"Response headers: {dict(response.headers)}")
                print(f"Response body: {response.text}")
                
                # Accept all 2xx status codes as success
                if response.status_code < 200 or response.status_code >= 300:
                    error_detail = response.text
                    try:
                        error_json = response.json()
                        error_detail = json.dumps(error_json, indent=2)
                    except:
                        pass
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"API Error: {error_detail}"
                    )
                return response.json()
            except httpx.RequestError as e:
                print(f"\n=== Network Error ===")
                print(f"Error: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Network error: {str(e)}"
                )
    except Exception as e:
        print(f"\n=== General Error ===")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting result: {str(e)}"
        )

@router.post("")
async def create_result(result: ScrapingResultCreate, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Create a new scraping result
    """
    try:
        # Convert the result data to dict and ensure datetime is properly formatted
        result_data = result.dict()
        result_data['scraped_at'] = result_data['scraped_at'].isoformat()
        for agent in result_data['agent_status']:
            agent['start_time'] = agent['start_time'].isoformat()
            if agent.get('end_time'):
                agent['end_time'] = agent['end_time'].isoformat()
        
        print("\n=== Create Result Request ===")
        print(f"URL: {BOINGO_API_URL}/scraping-results")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        print(f"Request body: {json.dumps(result_data, indent=2)}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{BOINGO_API_URL}/scraping-results",
                    headers={
                        "Authorization": f"Bearer {credentials.credentials}",
                        "Content-Type": "application/json"
                    },
                    json=result_data
                )
                
                print("\n=== Create Result Response ===")
                print(f"Status code: {response.status_code}")
                print(f"Response headers: {dict(response.headers)}")
                print(f"Response body: {response.text}")
                
                # Accept both 200 and 201 as success codes
                if response.status_code not in [200, 201]:
                    error_detail = response.text
                    try:
                        error_json = response.json()
                        error_detail = json.dumps(error_json, indent=2)
                    except:
                        pass
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"API Error: {error_detail}"
                    )
                return response.json()
            except httpx.RequestError as e:
                print(f"\n=== Network Error ===")
                print(f"Error: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Network error: {str(e)}"
                )
    except Exception as e:
        print(f"\n=== General Error ===")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating result: {str(e)}"
        )

@router.put("")
async def update_result(result: ScrapingResultUpdate, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Update an existing scraping result
    """
    try:
        # Convert the result data to dict and ensure datetime is properly formatted
        result_data = result.dict()
        result_data['scraped_at'] = result_data['scraped_at'].isoformat()
        result_data['last_updated'] = result_data['last_updated'].isoformat()
        
        print("\n=== Update Result Request ===")
        print(f"URL: {BOINGO_API_URL}/scraping-results")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        print(f"Request body: {json.dumps(result_data, indent=2)}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    f"{BOINGO_API_URL}/scraping-results",
                    headers={
                        "Authorization": f"Bearer {credentials.credentials}",
                        "Content-Type": "application/json"
                    },
                    json=result_data
                )
                
                print("\n=== Update Result Response ===")
                print(f"Status code: {response.status_code}")
                print(f"Response headers: {dict(response.headers)}")
                print(f"Response body: {response.text}")
                
                # Accept all 2xx status codes as success
                if response.status_code < 200 or response.status_code >= 300:
                    error_detail = response.text
                    try:
                        error_json = response.json()
                        error_detail = json.dumps(error_json, indent=2)
                    except:
                        pass
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"API Error: {error_detail}"
                    )
                return response.json()
            except httpx.RequestError as e:
                print(f"\n=== Network Error ===")
                print(f"Error: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Network error: {str(e)}"
                )
    except Exception as e:
        print(f"\n=== General Error ===")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error updating result: {str(e)}"
        )

@router.delete("")
async def delete_result(result: ScrapingResultDelete, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Delete a scraping result
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{BOINGO_API_URL}/scraping-results",
                headers={"Authorization": f"Bearer {credentials.credentials}"},
                json=result.dict()
            )
            # Accept all 2xx status codes as success
            if response.status_code < 200 or response.status_code >= 300:
                raise HTTPException(status_code=response.status_code, detail=response.text)
            return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting result: {str(e)}") 