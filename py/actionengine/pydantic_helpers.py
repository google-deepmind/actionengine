# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import inspect
import types
import typing
from types import NoneType, UnionType
from typing import Any, ClassVar
from typing import Optional

import numpy as np
import ormsgpack
from pydantic import BaseModel
from pydantic.v1.decorator import (
    ALT_V_ARGS,
    ALT_V_KWARGS,
    V_POSITIONAL_ONLY_NAME,
    V_DUPLICATE_KWARGS,
)


class ActSchema(BaseModel):
    _act_schema_name: ClassVar[str | None] = None

    class Config:
        arbitrary_types_allowed = True


PYDANTIC2_SPECIAL_NAMES = {
    "",
}


class AnyModel(BaseModel):
    pass


def packb_with_basic_collections(obj: Any, **kwargs):
    if isinstance(obj, BaseModel):
        component_name = get_component_name(type(obj))
        _get_component_registry()[component_name] = type(obj)

    def recursively_pack_collections(obj: Any):
        if isinstance(obj, BaseModel):
            fields = obj.model_dump()
            for key, value in fields.items():
                if key.startswith("__"):
                    continue

                fields[key] = recursively_pack_collections(value)
            return fields

        elif isinstance(obj, dict):
            for key, nested_obj in obj.items():
                obj[key] = recursively_pack_collections(nested_obj)

        elif isinstance(obj, list):
            return list(
                recursively_pack_collections(nested_obj) for nested_obj in obj
            )

        elif isinstance(obj, tuple):
            return tuple(
                recursively_pack_collections(nested_obj) for nested_obj in obj
            )

        elif isinstance(obj, set):
            return set(
                recursively_pack_collections(nested_obj) for nested_obj in obj
            )

        return obj

    obj = recursively_pack_collections(obj)

    return ormsgpack.packb(
        obj,
        option=(
            ormsgpack.OPT_SERIALIZE_NUMPY
            | ormsgpack.OPT_SERIALIZE_PYDANTIC
            | ormsgpack.OPT_UTC_Z
        ),
        **kwargs,
    )


packb = packb_with_basic_collections


def _numpy_outer_type_to_dtype(outer_type: Any) -> np.dtype | None:
    dtype: np.dtype | None = None

    assert issubclass(typing.get_origin(outer_type), np.ndarray)
    type_args = typing.get_args(outer_type)
    if type_args:
        dtype: np.dtype = typing.get_args(type_args[-1])[-1]

    return dtype


def _deserialize_unpacked_numpy(
    data: list, model: Optional[type[np.ndarray] | types.GenericAlias] = None
):
    if issubclass(model, np.ndarray):
        dtype = np.uint8
    else:
        dtype = _numpy_outer_type_to_dtype(model)

    return np.array(data, dtype=dtype)


PYDANTIC_SPECIAL_NAMES = {
    ALT_V_ARGS,
    ALT_V_KWARGS,
    V_POSITIONAL_ONLY_NAME,
    V_DUPLICATE_KWARGS,
}


def _coerce_elements_of_basic_collections(
    unpacked: list | dict | tuple | set,
    model: type | BaseModel | None,
    validate: bool = True,
):
    model_origin = typing.get_origin(model)
    if model_origin is None:
        model_origin = model

    if model_origin is list:
        args = typing.get_args(model)
        if not args:
            element_type = None
        else:
            element_type = args[0]

        return [
            deserialize_unpacked(element, element_type, validate=validate)
            for element in unpacked
        ]

    if model_origin is tuple:
        element_types = typing.get_args(model)
        if not element_types:
            return tuple(
                deserialize_unpacked(element, None, validate=validate)
                for element in unpacked
            )

        return tuple(
            deserialize_unpacked(element, element_type, validate=validate)
            for element, element_type in zip(unpacked, element_types)
        )

    if model_origin is dict:
        args = typing.get_args(model)
        if not args:
            key_model = None
            value_model = None
        else:
            key_model, value_model = args

        result = {}
        for key, value in unpacked.items():
            key = deserialize_unpacked(key, key_model, validate=validate)
            value = deserialize_unpacked(value, value_model, validate=validate)
            result[key] = value

        return result

    if model_origin is set:
        args = typing.get_args(model)
        if not args:
            element_type = None
        else:
            element_type = args[0]

        return {
            deserialize_unpacked(element, element_type, validate=validate)
            for element in unpacked
        }

    raise TypeError(
        f"Unsupported collection type {model_origin}. "
        "Only list, tuple, dict, and set are supported."
    )


def deserialize_unpacked(
    unpacked,
    model: type | BaseModel | None = None,
    validate: bool = True,
    required: bool = True,
):
    if model is None:
        return unpacked

    model_origin = typing.get_origin(model)
    if model_origin is None:
        model_origin = model

    if inspect.isclass(model_origin):
        if unpacked is None:
            if not required:
                return None
            if validate:
                raise ValueError("Required field is None")

        # try to construct the model from the unpacked data
        # first, try known coercions, then try the model constructor
        # if that fails, raise a TypeError
        if issubclass(model_origin, np.ndarray):
            return _deserialize_unpacked_numpy(unpacked, model)
        elif issubclass(model_origin, datetime.datetime):
            return datetime.datetime.fromisoformat(unpacked)
        elif not issubclass(model_origin, BaseModel):
            if model_origin in (list, dict, tuple, set):
                unpacked = _coerce_elements_of_basic_collections(
                    unpacked, model, validate=validate
                )

            if model_origin == typing.Any or isinstance(unpacked, model_origin):
                return unpacked

            if model_origin is UnionType and isinstance(model, UnionType):
                possible_types = model.__args__
                if NoneType in possible_types and unpacked is None:
                    return None
                for possible_type in possible_types:
                    try:
                        return deserialize_unpacked(
                            unpacked, possible_type, validate=validate
                        )
                    except (TypeError, AttributeError, ValueError):
                        pass

            try:
                return model(unpacked)
            except (TypeError, AttributeError, ValueError):
                raise TypeError(
                    f"Cannot construct a {model} from a {type(unpacked)} object."
                )

    if model_origin == typing.Any:
        return unpacked

    if isinstance(unpacked, model_origin):
        if validate:
            unpacked = type(unpacked)(**unpacked.model_dump())
        return unpacked

    for field_name, field in model.model_fields.items():
        if field_name in PYDANTIC_SPECIAL_NAMES | {"args", "kwargs"}:
            continue

        if field_name.startswith("__"):
            continue
        # if field.type_ is np.ndarray:
        #     unpacked[field_name] = _deserialize_unpacked_numpy(unpacked[field_name], field.outer_type_)

        unpacked[field_name] = deserialize_unpacked(
            unpacked.get(field_name, field.default),
            field.annotation,
            validate=validate,
            required=field.is_required(),
        )

    if validate:
        return model(**unpacked)
    else:
        return model.model_construct(**unpacked)


def unpackb(
    data: bytes,
    model: type | BaseModel | None = None,
    option: int | None = None,
):
    unpacked = ormsgpack.unpackb(data, option=option)
    return deserialize_unpacked(unpacked, model)


def get_component_name(component_type: type) -> str:
    if issubclass(component_type, ActSchema):
        act_schema_name = getattr(component_type, "_act_schema_name", None)
        name_parts = (
            ["__d_"] + [component_type.__qualname__]
            if act_schema_name is None
            else [act_schema_name]
        )
        return "".join(name_parts)

    module = component_type.__module__
    if module == "builtins" or module == "__main__":
        return component_type.__qualname__

    return module + "." + component_type.__qualname__


def _get_component_registry() -> dict[str, type]:
    if not hasattr(_get_component_registry, "_registry"):
        _get_component_registry._registry = {}
    return getattr(_get_component_registry, "_registry")


def base_model_to_bytes(component: BaseModel) -> bytes:
    component_name = get_component_name(type(component))
    return ormsgpack.packb((component_name, packb(component)))


def bytes_to_base_model(data: bytes) -> BaseModel:
    component_name, packed_data = ormsgpack.unpackb(data)

    model = _get_component_registry().get(component_name)
    if model is None:
        raise ValueError(f"Unknown component name: {component_name}")

    return unpackb(packed_data, model=model)
