import os
import asyncio
from datetime import datetime
from symbios import Symbios
from symbios.utils import Props
from symbios.queue import Queue
from symbios.message import SendingMessage, IncomingMessage

class Main:
    '''The main class used from the entrypoint file.
    '''
    
    @staticmethod
    async def run() -> None:
        '''The main method that will be called by the entrypoint file.
        Initialize the RPC server and wait for client requests.
        '''
        
        broker: Symbios = Symbios(
            host=os.environ['RMQ_HOST'],
            vhost=os.environ['RMQ_VHOST'],
            user=os.environ['RMQ_USERNAME'], 
            password=os.environ['RMQ_PASSWORD']
        )

        print(f'{datetime.now()} > [*] Starting to listen requests...')
        await broker.listen(
            Main._on_receive_handler, 
            queue=Queue(os.environ['RPC_QUEUE']), 
            no_ack=True
        )

    @staticmethod
    async def _on_receive_handler(
        broker: Symbios, 
        message: IncomingMessage
    ) -> None:
        print(
            f'{datetime.now()} > [x] Received requests. '
            f'Message body: {message.deserialized}.'
        )
        res: float = await Main._compute(message.deserialized)

        await broker.emit(
            SendingMessage(res),
            routing_key=message.props.reply_to,
            props=Props(correlation_id=message.props.correlation_id),
        )
        print(f'{datetime.now()} > [x] Response sent.')

    @staticmethod
    async def _compute(calc: str) -> float:
        t_sleep: int = 2 if '*' not in calc else 6
        await asyncio.sleep(t_sleep)

        return eval(calc)
