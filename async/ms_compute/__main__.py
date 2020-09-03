import asyncio
from ms_compute.main import Main

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(Main.run())
    loop.run_forever()