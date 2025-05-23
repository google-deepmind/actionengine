from google import genai


def get_gemini_client(api_key: str):
    return genai.client.AsyncClient(
        genai.client.BaseApiClient(
            api_key=api_key,
        )
    )
