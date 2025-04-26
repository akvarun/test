#!/usr/bin/env python3
# rabbitmq_quicktest.py
import pika
import sys

def test_connection(host):
    try:
        # 1. Connect to server
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=5672,
                                    credentials=pika.PlainCredentials('guest', 'guest'))
        print(f"✅ Connected to {host}")

        # 2. Set up temporary queue
        channel = connection.channel()
        channel.exchange_declare(exchange='ufo', exchange_type='fanout')
        queue = channel.queue_declare(queue='', exclusive=True)
        channel.queue_bind(exchange='ufo', queue=queue.method.queue)
        print("✅ Queue ready - waiting for messages... (Ctrl+C to stop)")

        # 3. Print any incoming messages
        def callback(ch, method, properties, body):
            print(f"📩 Received: {body.decode()}")

        channel.basic_consume(queue=queue.method.queue,
                            on_message_callback=callback,
                            auto_ack=True)
        channel.start_consuming()

    except Exception as e:
        print(f"❌ Failed to connect: {str(e)}")
        print("Debug tips:")
        print("- Try different host (cpu002, gpu001, etc.)")
        print("- Check your internet/VPN connection")
        print("- Verify port 5672 is open")

if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else 'cpu001.cm.cluster'
    test_connection(host)
