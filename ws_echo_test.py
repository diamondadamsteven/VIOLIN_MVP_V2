import asyncio, websockets

HOST = "192.168.1.27"  # <-- your server IP
PORT = 7070
URI  = f"ws://{HOST}:{PORT}/ws/echo"

async def go():
    print("Connecting to", URI)
    # try without subprotocols first; if that fails, try subprotocols=["v1"]
    async with websockets.connect(URI) as ws:
        # expect the server banner (we re-enabled it)
        msg = await ws.recv()
        print("Server banner:", msg)
        await ws.send("ping")
        echo = await ws.recv()
        print("Echo:", echo)

asyncio.run(go())
