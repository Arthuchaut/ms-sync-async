import os
import time
import asyncio
from datetime import datetime
from typing import List, Coroutine
from symbios import Symbios
from symbios.message import SendingMessage

class Main:
    '''Main class of the async client microservice.
    Will be used from the entrypoint file.
    '''
    
    @staticmethod
    async def run() -> None:
        '''The main method that will be called by the entrypoint file.
        Do multiple RPC calls and display the result in console.
        '''
        
        broker: Symbios = Symbios(
            host=os.environ['RMQ_HOST'],
            vhost=os.environ['RMQ_VHOST'],
            user=os.environ['RMQ_USERNAME'], 
            password=os.environ['RMQ_PASSWORD']
        )
        t0: float = time.time()

        print(
            f'{datetime.now()} > [*] Sending requests '
            f'and waiting for responses...'
        )
        tasks_queue: List[Coroutine] = broker.rpc.multi_calls([
            broker.rpc.call(
                SendingMessage('1+1'),
                routing_key=os.environ['RPC_QUEUE']
            ),
            broker.rpc.call(
                SendingMessage('4*5'),
                routing_key=os.environ['RPC_QUEUE']
            ),
            broker.rpc.call(
                SendingMessage('6*4'),
                routing_key=os.environ['RPC_QUEUE']
            ),
            broker.rpc.call(
                SendingMessage('10*5'),
                routing_key=os.environ['RPC_QUEUE']
            ),
            broker.rpc.call(
                SendingMessage('2-6'),
                routing_key=os.environ['RPC_QUEUE']
            ),
        ])

        for task in tasks_queue:
            print(
                f'{datetime.now()} > [x] Received response '
                f'from {task.get_coro()}. '
                f'Message body: {(await task).deserialized}'
            )

        print(
            f'{datetime.now()} > [x] End of process. '
            f'Total duration: {round(time.time() - t0, 2)}s'
        )