from .diffusion_client import DiffusionClient


def run_inference(client: DiffusionClient, prompt: str, n: int):
    images = [result.encoded_image for result in client.draw(n * [prompt], skip_decoding=True)]

    return images
