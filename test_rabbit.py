#!/usr/bin/env python3
"""
RabbitMQ Cluster Connectivity Test
Author: MIB Agent
Usage: python3 rabbitmq_test.py --host cpu001.cm.cluster
"""

import pika
import json
import argparse
from datetime import datetime

def test_rabbitmq_connection(host):
    try:
        # 1. Test basic connection
        print(f"🔌 Attempting connection to {host}...")
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=5672,
                credentials=credentials,
                heartbeat=600
            )
        )
        print("✅ Connection established successfully")

        # 2. Test channel and exchange
        channel = connection.channel()
        channel.exchange_declare(exchange='ufo', exchange_type='fanout')
        print("✅ UFO exchange verified")

        # 3. Test temporary queue
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange='ufo', queue=queue_name)
        print(f"✅ Temporary queue {queue_name} created and bound")

        # 4. Set up test consumer
        def callback(ch, method, properties, body):
            try:
                msg = json.loads(body)
                print(f"\n📡 Received message @ {datetime.now().isoformat()}:")
                print(json.dumps(msg, indent=2))
            except json.JSONDecodeError:
                print(f"⚠️ Malformed message: {body.decode()}")

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=True
        )

        # 5. Run with timeout
        print("\n🚀 Listening for messages (Ctrl+C to exit)...")
        print("⚠️ If no messages appear within 60s, the cluster may not be broadcasting")
        channel.start_consuming()

    except Exception as e:
        print(f"❌ Connection failed: {type(e).__name__}: {e}")
        print("\nDebug Checklist:")
        print("1. Verify hostname is correct (try ping)")
        print("2. Check VPN/network connectivity")
        print("3. Try alternate hosts (cpu002/gpu001-022)")
        print("4. Confirm port 5672 is unblocked")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='cpu001.cm.cluster', 
                       help='RabbitMQ host (e.g., cpu001.cm.cluster)')
    args = parser.parse_args()
    
    try:
        test_rabbitmq_connection(args.host)
    except KeyboardInterrupt:
        print("\n🔴 Test terminated by user")
