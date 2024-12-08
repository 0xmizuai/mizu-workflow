from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Annotated
from app.db.query import Connection, save_query
import jwt
import os
from base64 import b64decode
from contextlib import asynccontextmanager
import uvicorn

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = Connection()
    yield

app = FastAPI(lifespan=lifespan)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()
# Get the public key from environment variable
PUBLIC_KEY = b64decode(os.getenv('JWT_PUBLIC_KEY', ''))

async def get_publisher(credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]) -> str:
    try:
        token = credentials.credentials
        payload = jwt.decode(
            token,
            PUBLIC_KEY,
            algorithms=["EdDSA"]
        )
        # Assuming the publisher is stored in the 'sub' claim
        if 'sub' not in payload:
            raise HTTPException(status_code=401, detail="Publisher not found in token")
        return payload['sub']
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

@app.get("/")
async def root():
    return {"status": "ok"}

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/submit_query")
async def submit_query(
    dataset: str, 
    query: str, 
    publisher: Annotated[str, Depends(get_publisher)]
):
    with app.state.db.get_pg_connection() as cur:
        query_id = save_query(cur, dataset, query, publisher)
        return {
            "query_id": query_id,
            "dataset": dataset,
            "query": query,
            "publisher": publisher,
            "progress": "pending",
            "status": "saved"
        }

def start():
    """Start production server"""
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )

def start_dev():
    """Start development server with hot reload"""
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["app"],
    )

if __name__ == "__main__":
    start_dev()
