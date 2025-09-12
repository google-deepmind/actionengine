import base64

import actionengine
from pydantic import BaseModel, Field


class ChunkMetadata(BaseModel):
    mimetype: str = Field(..., description="MIME type of the chunk")
    attributes: dict[str, bytes] = Field(
        ..., description="Attributes of the chunk"
    )

    @staticmethod
    def copy_from(metadata: actionengine.ChunkMetadata):
        return ChunkMetadata(
            mimetype=metadata.mimetype, attributes=metadata.attributes
        )

    @staticmethod
    def from_json(data: dict) -> "ChunkMetadata":
        mimetype = data.get("mimetype", "")
        b64 = data.get("b64", False)
        attributes = dict()
        for k, v in data.get("attributes", {}).items():
            if isinstance(v, bytes):
                attributes[k] = v
                continue
            if b64:
                attributes[k] = base64.b64decode(v)
            else:
                attributes[k] = v.encode("utf-8")

        return ChunkMetadata(
            mimetype=mimetype,
            attributes=attributes,
        )

    def to_json(self, b64: bool = False) -> dict:
        attributes = dict()
        for k, v in self.attributes.items():
            if isinstance(v, str):
                v = v.encode("utf-8")
            if b64:
                attributes[k] = base64.b64encode(v).decode("utf-8")
            else:
                attributes[k] = v.decode("utf-8", errors="ignore")
        return {
            "mimetype": self.mimetype,
            "attributes": attributes,
        }


class Chunk(BaseModel):
    data: bytes | None = Field(..., description="Data of the chunk")
    metadata: ChunkMetadata | None = Field(
        None, description="Metadata of the chunk"
    )
    ref: str | None = Field(..., description="Reference of the chunk")

    @staticmethod
    def copy_from(chunk: actionengine.Chunk):
        metadata = (
            ChunkMetadata.copy_from(chunk.metadata)
            if chunk.metadata is not None
            else None
        )
        return Chunk(
            data=chunk.data or None, metadata=metadata, ref=chunk.ref or None
        )

    @staticmethod
    def from_json(data: dict) -> "Chunk":
        metadata = None
        if "metadata" in data and isinstance(data["metadata"], dict):
            metadata = ChunkMetadata.from_json(data["metadata"])
        chunk_data = data.get("data", None)
        if isinstance(chunk_data, str):
            chunk_data = base64.b64decode(chunk_data)
        return Chunk(
            data=chunk_data,
            metadata=metadata,
            ref=data.get("ref", None),
        )


class Port(BaseModel):
    name: str = Field(..., description="Name of the port")
    id: str = Field(..., description="ID of the port")

    @staticmethod
    def copy_from(port: actionengine.Port):
        return Port(name=port.name, id=port.id)


class PortSchema(BaseModel):
    name: str = Field(..., description="Name of the port")
    mimetype: str = Field(..., description="MIME type of the port")


class ActionSchema(BaseModel):
    name: str = Field(..., description="Name of the action")
    inputs: list[PortSchema] = Field(..., description="List of input ports")
    outputs: list[PortSchema] = Field(..., description="List of output ports")

    @staticmethod
    def copy_from(schema: actionengine.ActionSchema):
        inputs = [
            PortSchema(name=name, mimetype=mimetype)
            for name, mimetype in schema.inputs.items()
        ]
        outputs = [
            PortSchema(name=name, mimetype=mimetype)
            for name, mimetype in schema.outputs.items()
        ]
        return ActionSchema(name=schema.name, inputs=inputs, outputs=outputs)
