from .diffusion_client import DiffusionClient


def run_inference(client: DiffusionClient, prompt: str, n: int):
    images = client.draw(n*[prompt], return_encoded=True)

    return images
