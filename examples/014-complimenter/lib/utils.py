import uuid

import actionengine
import asyncio
import io

import requests
from PIL import Image


def get_device():
    import torch

    if torch.cuda.is_available():
        return "cuda"
    elif torch.backends.mps.is_available():
        return "mps"
    return "cpu"


async def load_image(url: str) -> Image.Image:
    response = await asyncio.to_thread(requests.get, url, stream=True)
    image = await asyncio.to_thread(Image.open, response.raw)
    width, height = image.size
    factor = min(width, height) / 512
    if factor > 1:
        image = await asyncio.to_thread(
            image.resize, (int(width / factor), int(height / factor))
        )
    return image


async def image_to_bytes(image: Image.Image) -> bytes:
    with io.BytesIO() as output:
        await asyncio.to_thread(image.save, output, format="PNG")
        return output.getvalue()


class AEInstance:
    def __init__(
        self,
        action_registry: actionengine.ActionRegistry,
        session: actionengine.Session | None = None,
        node_map: actionengine.NodeMap | None = None,
        stream: actionengine.stream.WireStream | None = None,
    ):
        self.registry = action_registry
        self.node_map = node_map or actionengine.NodeMap()
        self.session = session or actionengine.Session(
            self.node_map, self.registry
        )
        self.stream = stream

    def make_action(self, action_name: str) -> actionengine.Action:
        action = self.registry.make_action(
            action_name,
            str(uuid.uuid4()),
            node_map=self.node_map,
            session=self.session,
            stream=self.stream,
        )
        return action

    def run(self, action_name: str) -> actionengine.Action:
        return self.make_action(action_name).run()

    async def call(self, action_name: str) -> actionengine.Action:
        action = self.make_action(action_name)
        await action.call()
        return action
