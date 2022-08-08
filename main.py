from enum import Enum
from uuid import uuid4
import base64
import os

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse

from server import run_inference
from server.diffusion_client import DiffusionClient
import gc

app = FastAPI()



origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://laiogen.vercel.app",
    "173.245.48.0/20",
    "103.21.244.0/22",
    "103.22.200.0/22",
    "103.31.4.0/22",
    "141.101.64.0/18",
    "108.162.192.0/18",
    "190.93.240.0/20",
    "188.114.96.0/20",
    "197.234.240.0/22",
    "198.41.128.0/17",
    "162.158.0.0/15",
    "104.16.0.0/13",
     "104.24.0.0/14",
    "172.64.0.0/13",
    "131.0.72.0/22",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Model(str, Enum):
    DIFFUSION = "diffusion"

diffusion_client = None
diffusion_client_pid = None


@app.get("/generate")
async def generate(prompt: str, n: int,  model: Model):
    if not diffusion_client:
        client = DiffusionClient(
            initial_peers=[
                "/ip4/193.106.95.184/tcp/31234/p2p/Qmas1tApYHyNWXAMoJ9pxkAWBXcy4z11yquoAM3eiF1E86",
                "/ip4/193.106.95.184/tcp/31235/p2p/QmYN4gEa3uGVcxqjMznr5vEG7DUBGUWZgT98Rnrs6GU4Hn",
            ]
        )
        
    job_id = str(uuid4())
    images = list(map(base64.b64encode, run_inference(client, prompt, n)))

    return {"job_id": job_id, "images": images}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        port=80,
        host="0.0.0.0",
        reload=True,
    )
