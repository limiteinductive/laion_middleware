import base64
import gc
import io
import os
<<<<<<< HEAD
from enum import Enum
from pathlib import Path
from uuid import uuid4
=======
import threading
>>>>>>> e46591d8d65e4b584a25cc55329da53525c8b6ed

import PIL.Image as Image
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse

from server import run_inference
from server.diffusion_client import DiffusionClient

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
diffusion_client_lock = threading.Lock()


def save_images(images: list, job_id: str, path: str):
    for i, image in enumerate(images):
        image = Image.open(io.BytesIO(image))
        image.save((Path(path) / "_".join([job_id, str(i).zfill(2)])).with_suffix('.webp'))


@app.get("/generate")
<<<<<<< HEAD
async def generate(prompt: str, n: int, model: Model):
    global diffusion_client, diffusion_client_pid
    if diffusion_client is None or diffusion_client_pid != os.getpid():
        diffusion_client = DiffusionClient(
            initial_peers=[
                '/ip4/193.106.95.184/tcp/31334/p2p/QmRbeBn2noC63PWHAM2w4mQCrjLFks2vc4Dgy1YooEpUYJ',
                '/ip4/193.106.95.184/tcp/31335/p2p/Qmf3DM44osRjP2xFmomh8oH8HnwLDV9ePDMSvGo5JtjEuL',
            ]
        )
        diffusion_client_pid = os.getpid()

=======
def generate(prompt: str, n: int,  model: Model):
    global diffusion_client, diffusion_client_pid
    with diffusion_client_lock:
        if diffusion_client is None or diffusion_client_pid != os.getpid():
            diffusion_client = DiffusionClient(
                initial_peers=[
                    "/ip4/193.106.95.184/tcp/31234/p2p/Qmas1tApYHyNWXAMoJ9pxkAWBXcy4z11yquoAM3eiF1E86",
                    "/ip4/193.106.95.184/tcp/31235/p2p/QmYN4gEa3uGVcxqjMznr5vEG7DUBGUWZgT98Rnrs6GU4Hn",
                ]
            )
            diffusion_client_pid = os.getpid()
        
>>>>>>> e46591d8d65e4b584a25cc55329da53525c8b6ed
    job_id = str(uuid4())
    images = run_inference(diffusion_client, prompt, n)
    print(images)
    save_images(images, job_id, path="/root/images")

    encoded_images = list(map(base64.b64encode, images))

    return {"job_id": job_id, "images": encoded_images}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        port=80,
        host="0.0.0.0",
        reload=True,
    )
