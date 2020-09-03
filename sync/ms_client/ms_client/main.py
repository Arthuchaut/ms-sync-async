import time
import os
from datetime import datetime
from .rpc_compute_client import RpcComputeClient

class Main:
    '''The main class used from the entrypoint file.
    '''
    
    @staticmethod
    def run() -> None:
        '''The main method that will be called by the entrypoint file.
        Do RPC calls and display the responses in console.
        '''
        
        rpc_com: RpcComputeClient = RpcComputeClient(
            host=os.environ['RMQ_HOST'],
            vhost=os.environ['RMQ_VHOST'],
            username=os.environ['RMQ_USERNAME'], 
            password=os.environ['RMQ_PASSWORD']
        )
        calcs: List[str] = [
            '1+1',
            '4*5',
            '2-6'
        ]
        t0: float = time.time()

        for calc in calcs:
            print(
                f'{datetime.now()} > [*] Sending request for {calc} '
                f'and waiting for responses...'
            )
            res: float = rpc_com.call(calc)
            print(
                f'{datetime.now()} > [x] Received response. '
                f'Message body: {res}'
            )

        print(
            f'{datetime.now()} > [x] End of process. '
            f'Total duration: {round(time.time() - t0, 2)}s'
        )