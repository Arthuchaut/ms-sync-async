import os
import time
from datetime import datetime
from typing import Any
import pika

class Main:
    @staticmethod
    def run() -> None:
        channel: pika.channel.Channel = Main._connect()
        channel.queue_declare(queue=os.environ['RPC_QUEUE'])
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=os.environ['RPC_QUEUE'], 
            on_message_callback=Main._on_request_handler
        )
        print(f'{datetime.now()} > [*] Starting to listen requests...')
        channel.start_consuming()

    @staticmethod
    def _connect() -> pika.channel.Channel:
        conn: pika.connection.Connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.environ['RMQ_HOST'], 
                virtual_host=os.environ['RMQ_VHOST'], 
                credentials=pika.PlainCredentials(
                    os.environ['RMQ_USERNAME'], 
                    os.environ['RMQ_PASSWORD']
                )
            )
        )

        return conn.channel()

    @staticmethod
    def _on_request_handler(
        channel: pika.channel.Channel, 
        method: Any, 
        props: pika.BasicProperties, 
        body: bytes
    ) -> None:
        print(
            f'{datetime.now()} > [x] Received requests. '
            f'Message body: {body.decode(encoding="UTF-8")}.'
        )

        res: float = Main._compute(body.decode(encoding='UTF-8'))
        channel.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id
            ),
            body=str(res)
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f'{datetime.now()} > [x] Response sent.')

    @staticmethod
    def _compute(calc: str) -> float:
        t_sleep: int = 2 if '*' not in calc else 6
        time.sleep(t_sleep)

        return eval(calc)
