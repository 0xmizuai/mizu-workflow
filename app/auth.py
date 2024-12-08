from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Annotated
import jwt
import os

# Security configuration
PUBLIC_KEY = os.getenv("JWT_PUBLIC_KEY")
INTERNAL_SERVICE_KEY = os.environ["INTERNAL_SERVICE_KEY"]
security = HTTPBearer()


async def get_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]
) -> str:
    """Verify JWT token and extract publisher from claims."""
    try:
        token = credentials.credentials
        payload = jwt.decode(token, PUBLIC_KEY, algorithms=["EdDSA"])
        if "sub" not in payload:
            raise HTTPException(status_code=401, detail="Publisher not found in token")
        return payload["sub"]
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")


async def verify_internal_service(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]
) -> bool:
    """Verify internal service API key."""
    token = credentials.credentials
    if token != INTERNAL_SERVICE_KEY:
        raise HTTPException(status_code=403, detail="Invalid internal service key")
    return True
