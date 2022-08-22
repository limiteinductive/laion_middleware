from .diffusion_client import DiffusionClient


def run_inference(client: DiffusionClient, prompt: str, n: int, seed: int=None):
    return client.draw(n * [prompt], seed=seed, skip_decoding=True)
