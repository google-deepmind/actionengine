import sys
import ssl
import json
import asyncio
import logging
import websockets


logger = logging.getLogger("websockets")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

clients = {}


async def handle_websocket(connection: websockets.ServerConnection):
    client_id = None
    try:
        client_id = connection.request.path.split("/")[1]
        print("Client {} connected".format(client_id))

        clients[client_id] = connection
        while True:
            data = await connection.recv()
            print("Client {} << {}".format(client_id, data))
            message = json.loads(data)
            destination_id = message["id"]
            destination_websocket = clients.get(destination_id)
            if destination_websocket:
                message["id"] = client_id
                data = json.dumps(message)
                print("Client {} >> {}".format(destination_id, data))
                await destination_websocket.send(data)
            else:
                print("Client {} not found".format(destination_id))
                await connection.close(code=4000, reason="Peer not found")

    except Exception as e:
        print(e)

    finally:
        if client_id:
            del clients[client_id]
            print("Client {} disconnected".format(client_id))


async def main():
    # Usage: ./server.py [[host:]port] [SSL certificate file]
    endpoint_or_port = sys.argv[1] if len(sys.argv) > 1 else "8000"
    ssl_cert = sys.argv[2] if len(sys.argv) > 2 else None

    endpoint = (
        endpoint_or_port
        if ":" in endpoint_or_port
        else "127.0.0.1:" + endpoint_or_port
    )

    if ssl_cert:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(
            "/home/alex/demos.helena.direct/cert.pem",
            "/home/alex/demos.helena.direct/privkey.pem",
        )
    else:
        ssl_context = None

    print("Listening on {}".format(endpoint))
    host, port = endpoint.rsplit(":", 1)

    server = await websockets.serve(
        handle_websocket, host, int(port), ssl=ssl_context
    )
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
