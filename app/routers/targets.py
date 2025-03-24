from fastapi import APIRouter, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials
import httpx
import json
from datetime import datetime
from ..models.models import (
    ScrapingTargetCreate, ScrapingTargetUpdate,
    ScrapingTargetDelete, ScrapingTargetPause
)
from ..core.config import BOINGO_API_URL
from .auth import security

router = APIRouter(
    prefix="/scraping-target",
    tags=["Scraping Target API"],
    responses={404: {"description": "Not found"}},
)

@router.get("")
async def get_all_targets(credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Get all scraping targets
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{BOINGO_API_URL}/scraping-target",
                headers={"Authorization": f"Bearer {credentials.credentials}"}
            )
            # Accept all 2xx status codes as success
            if response.status_code < 200 or response.status_code >= 300:
                raise HTTPException(status_code=response.status_code, detail=response.text)
            return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting targets: {str(e)}")

@router.get("/{target_id}")
async def get_target_by_id(target_id: str, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Get scraping target by ID
    """
    try:
        print(f"\n=== Get Target Request ===")
        print(f"Target ID: {target_id}")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{BOINGO_API_URL}/scraping-target/{target_id}",
                    headers={"Authorization": f"Bearer {credentials.credentials}"}
                )
                
                print(f"\n=== Get Target Response ===")
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
            detail=f"Error getting target: {str(e)}"
        )

@router.post("")
async def create_target(target: ScrapingTargetCreate, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Create a new scraping target
    """
    try:
        print("\n=== Create Target Request ===")
        print(f"URL: {BOINGO_API_URL}/scraping-target")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        print(f"Request body: {json.dumps(target.dict(), indent=2)}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{BOINGO_API_URL}/scraping-target",
                    headers={
                        "Authorization": f"Bearer {credentials.credentials}",
                        "Content-Type": "application/json"
                    },
                    json=target.dict()
                )
                
                print("\n=== Create Target Response ===")
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
            detail=f"Error creating target: {str(e)}"
        )

@router.put("")
async def update_target(target: ScrapingTargetUpdate, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Update an existing scraping target
    """
    try:
        print("\n=== Update Target Request ===")
        print(f"URL: {BOINGO_API_URL}/scraping-target")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        print(f"Request body: {json.dumps(target.dict(), indent=2)}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    f"{BOINGO_API_URL}/scraping-target",
                    headers={
                        "Authorization": f"Bearer {credentials.credentials}",
                        "Content-Type": "application/json"
                    },
                    json=target.dict()
                )
                
                print("\n=== Update Target Response ===")
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
            detail=f"Error updating target: {str(e)}"
        )

@router.delete("")
async def delete_target(target: ScrapingTargetDelete, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Delete a scraping target
    """
    try:
        print("\n=== Delete Target Request ===")
        print(f"URL: {BOINGO_API_URL}/scraping-target")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        print(f"Request body: {json.dumps(target.dict(), indent=2)}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.delete(
                    f"{BOINGO_API_URL}/scraping-target",
                    headers={
                        "Authorization": f"Bearer {credentials.credentials}",
                        "Content-Type": "application/json"
                    },
                    json=target.dict()
                )
                
                print("\n=== Delete Target Response ===")
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
            detail=f"Error deleting target: {str(e)}"
        )

@router.post("/pause")
async def pause_target(target: ScrapingTargetPause, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Pause a scraping target
    """
    try:
        print("\n=== Pause Target Request ===")
        print(f"URL: {BOINGO_API_URL}/scraping-target/pause")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        print(f"Request body: {json.dumps(target.dict(), indent=2)}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{BOINGO_API_URL}/scraping-target/pause",
                    headers={
                        "Authorization": f"Bearer {credentials.credentials}",
                        "Content-Type": "application/json"
                    },
                    json=target.dict()
                )
                
                print("\n=== Pause Target Response ===")
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
            detail=f"Error pausing target: {str(e)}"
        )

@router.post("/unpause")
async def unpause_target(target: ScrapingTargetPause, credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Unpause a scraping target
    """
    try:
        print("\n=== Unpause Target Request ===")
        print(f"URL: {BOINGO_API_URL}/scraping-target/unpause")
        print(f"Authorization: Bearer {credentials.credentials[:20]}...")
        print(f"Request body: {json.dumps(target.dict(), indent=2)}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{BOINGO_API_URL}/scraping-target/unpause",
                    headers={
                        "Authorization": f"Bearer {credentials.credentials}",
                        "Content-Type": "application/json"
                    },
                    json=target.dict()
                )
                
                print("\n=== Unpause Target Response ===")
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
            detail=f"Error unpausing target: {str(e)}"
        ) 