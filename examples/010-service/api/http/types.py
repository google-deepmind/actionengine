from typing import Annotated

from fastapi import Header


ActionEngineApiKeyHeader = Annotated[str, Header(alias="X-AE-Api-Key")]
