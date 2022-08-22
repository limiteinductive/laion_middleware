import base64
import gc
import io
import os
from enum import Enum
from pathlib import Path
from uuid import uuid4
import threading
import time


import PIL.Image as Image
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse

from server import run_inference
from server.diffusion_client import DiffusionClient, NoModulesFound

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
        image.save(
            (Path(path) / "_".join([job_id, str(i).zfill(2)])).with_suffix(".webp")
        )


@app.get("/generate")
async def generate(prompt: str, n: int, seed: int=None, model: Model = "diffusion"):
    global diffusion_client, diffusion_client_pid
    if diffusion_client is None or diffusion_client_pid != os.getpid():
        diffusion_client = DiffusionClient(
            initial_peers=[
               "/ip4/34.79.100.149/tcp/38731/p2p/QmYiEc3moPrWcZoQZmXKXJjpH23REEMjNkL2xgzhpaQufa",
               #"/dns/2.tcp.ngrok.io/tcp/10359/p2p/QmYiEc3moPrWcZoQZmXKXJjpH23REEMjNkL2xgzhpaQufa"
            ]
        )
        diffusion_client_pid = os.getpid()

    job_id = str(uuid4())
    try: 
        images = run_inference(diffusion_client, prompt, n, seed=seed)
    except NoModulesFound:
        return "no modules found."
    print(images)
    save_images(images, job_id, path="/root/images")

    encoded_images = list(map(base64.b64encode, images))

    return {"job_id": job_id, "images": encoded_images}


@app.get("/html", response_class=HTMLResponse)
async def html(prompt: str, n: int, seed: int=None):
    start = time.time()

    while time.time() - start < 60:
        output = await generate(prompt, n, seed=seed, model="diffusion")

        if isinstance(output, dict):
            images = output["images"]
        else:
            time.sleep(1)
            continue

        body = "<body>"
        for image in images:
            body += f'<img src="data:image/png;base64, {str(image)[2:-1]}" /> '
        body += "</body>"

        return f"""
        <html>
            <head>
                <title>Images sucessefuly generated!</title>
            </head
            {body}
        </html>
        """
    return """
    <html>
        <body>
            No idle experts found...
        </body>
    </html>
    """


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        port=80,
        host="0.0.0.0",
        reload=True,
    )
