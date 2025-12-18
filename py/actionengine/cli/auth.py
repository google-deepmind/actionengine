from getpass import getpass

import requests
from pathlib import Path

from actionengine.cli.utils import API_ROOT


async def register(
    username: str,
    password: str,
    email: str,
    full_name: str = "",
    invitation_code: str | None = None,
) -> None:

    request = {
        "username": username,
        "password": password,
        "email": email,
        "full_name": full_name,
    }

    if invitation_code is not None:
        request["invitation_code"] = invitation_code

    response = requests.post(
        f"{API_ROOT}/users/",
        json=request,
    )
    response.raise_for_status()


async def create_api_key(
    username: str,
    password: str,
    name: str = "",
    description: str = "",
) -> str:

    response = requests.post(
        f"{API_ROOT}/auth/api-keys/",
        json={
            "username": username,
            "password": password,
            "name": name,
            "description": description,
        },
    )
    response.raise_for_status()

    key = response.text
    if key.startswith('"') and key.endswith('"'):
        key = key[1:-1]

    return key


def _validate_email(email: str) -> bool:
    if "@" not in email or "." not in email.split("@")[-1]:
        return False
    return True


async def register_command() -> None:
    print("Registering a new user for Action Engine.")

    username = input("Username: ").strip()
    if (
        any(c.isspace() for c in username)
        or not username
        or len(username) < 3
        or any(char in username for char in r'@\/:*?"<>|')
    ):
        print(
            "Invalid username. It must be at least 3 characters long, "
            "contain no spaces, and not include any of the following "
            'characters: @ \\ / : * ? " < > |'
        )
        return
    email = input("Email: ").strip()
    if not _validate_email(email):
        print("Invalid email address.")
        return
    full_name = input("Full Name (optional): ").strip()
    if len(full_name) > 255:
        print("Full name is too long (maximum 255 characters).")
        return
    print(
        "Your password must be at least 12 characters long, contain at least one "
        "uppercase letter, one lowercase letter, one digit, and one special "
        "character."
    )
    password = None
    while True:
        password = getpass("Enter password: ")
        if len(password) < 12:
            print("Password is too short. Please try again.")
            continue
        if not any(c.islower() for c in password):
            print(
                "Password must contain at least one lowercase letter. Please try again."
            )
            continue
        if not any(c.isupper() for c in password):
            print(
                "Password must contain at least one uppercase letter. Please try again."
            )
            continue
        if not any(c.isdigit() for c in password):
            print("Password must contain at least one digit. Please try again.")
            continue
        if not any(not c.isalnum() for c in password):
            print(
                "Password must contain at least one special character. Please try again."
            )
            continue
        break

    while True:
        confirm_password = getpass("Confirm password: ")
        if password != confirm_password:
            print("Passwords do not match.")
            return
        break

    try:
        await register(
            username=username,
            password=password,
            email=email,
            full_name=full_name,
            invitation_code=None,
        )
    except requests.HTTPError as e:
        http_status_code = e.response.status_code
        status = e.response.json()
        print(f"Error {http_status_code}: {status['message']}")
        return

    print(f"User '{username}' registered successfully. Creating an API key.")
    try:
        api_key = await create_api_key(
            username=username,
            password=password,
            name="Default API Key",
            description="API key created during registration.",
        )
    except requests.HTTPError as e:
        http_status_code = e.response.status_code
        status = e.response.json()
        print(f"Error {http_status_code}: {status['message']}")
        return

    key_path = Path.home() / ".actionengine" / "credentials"
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_text(f"{username}:{api_key}")
    print(f"API key created and saved to '{key_path}' alongside the username.")
