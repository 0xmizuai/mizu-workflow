from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Annotated
import jwt
import os

# Security configuration
API_SECRET_KEY = os.environ["API_SECRET_KEY"]
security = HTTPBearer()

async def verify_internal_service(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]
) -> bool:
    """Verify internal service API key."""
    token = credentials.credentials
    if token != API_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Invalid internal service key")
    return True
