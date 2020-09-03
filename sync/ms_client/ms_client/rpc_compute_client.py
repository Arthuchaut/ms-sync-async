import os
import uuid
from typing import Dict, Any
import pika

class RpcComputeClient:
    '''The RPC Compute client class definition.
    
    Attributes:
        **kwargs (Dict[str, str]): The broker parameters and
            the user credentials.
    '''
    
    def __init__(self, **kwargs: Dict[str, str]):
        '''The cronstructor method.
        '''
        
        self._conn: pika.connection.Connection = self._connect(**kwargs)
        self._channel: pika.channel.Channel = self._conn.channel()
        self._callback_queue: str = self._declare_queue()
        self._corrid: str = None
        self._start_consuming()

    def _connect(
        self, 
        *, 
        host: str, 
        vhost: str, 
        username: str, 
        password: str
    ) -> pika.connection.Connection:
        '''Method Description.
        Connect to the broker and return the connection pipe.
        
        Args:
            host (str): The broker host.
            vhost (str): The broker virtual host.
            username (str): The broker user name.
            password (str): The broker user password.
        
        Returns:
            pika.connection.Connection: The broker connection.
        '''
    
        return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host, 
                virtual_host=vhost, 
                credentials=pika.PlainCredentials(
                    username, 
                    password
                )
            )
        )

    def _declare_queue(self) -> str:
        '''The queue declaration method.
        Create or keeping alive a queue from the broker.
        
        Returns:
            str: The queue name.
        '''
        
        res: Any = self._channel.queue_declare(queue='', exclusive=True)

        return res.method.queue

    def _start_consuming(self) -> None:
        '''Consume a queue and wait for messages.
        '''
        
        self._channel.basic_consume(
            queue=self._callback_queue,
            on_message_callback=self._on_response_handler,
            auto_ack=True
        )

    def _on_response_handler(
        self, 
        channel: pika.channel.Channel, 
        method: Any, 
        props: pika.BasicProperties, 
        body: bytes
    ) -> None:
        '''The response handler method.
        The procedure that will be called when a message 
        arrives into the queue.
        Valuate the _response attribute from this class by the message body.
        
        Args:
            channel (pika.channel.Channel): The connection channel.
            method (Any): The message method.
            props (pika.BasicProperties): The message properties.
            body (bytes): The message body.
        '''
        
        if self._corrid == props.correlation_id:
            self._response = body

    def call(self, calc: str) -> float:
        '''The RPC call method.

        Args:
            calc (str): The mathematical operation to send to the computation server.
        
        Returns:
            float: The RPC server result.
        '''
        
        self._response = None
        self._corrid = str(uuid.uuid4())
        self._channel.basic_publish(
            exchange='',
            routing_key=os.environ['RPC_QUEUE'],
            properties=pika.BasicProperties(
                reply_to=self._callback_queue,
                correlation_id=self._corrid,
            ),
            body=str(calc)
        )

        while self._response is None:
            self._conn.process_data_events()

        return float(self._response)