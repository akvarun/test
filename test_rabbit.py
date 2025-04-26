#!/usr/bin/env python3
"""
RabbitMQ Cluster Auto-Discovery Test
Tests all possible hosts: cpu[001-002],gpu[001-022].cm.cluster
"""
import pika
import json
import argparse
from datetime import datetime
import concurrent.futures

def test_host(host):
    try:
        print(f"🔌 Testing {host}...", end=' ', flush=True)
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=5672,
                credentials=credentials,
                heartbeat=600,
                connection_attempts=3,
                retry_delay=1
            )
        )
        print("✅ Connected")
        
        # Set up consumer
        channel = connection.channel()
        channel.exchange_declare(exchange='ufo', exchange_type='fanout')
        queue = channel.queue_declare(queue='', exclusive=True)
        channel.queue_bind(exchange='ufo', queue=queue.method.queue)
        
        # Non-blocking message check
        method_frame, _, body = channel.basic_get(queue=queue.method.queue, auto_ack=True)
        if method_frame:
            print(f"📡 Received message on {host}:")
            try:
                print(json.dumps(json.loads(body), indent=2))
            except:
                print(body.decode())
        
        connection.close()
        return host
    except Exception as e:
        print(f"❌ Failed ({type(e).__name__})")
        return None

def find_active_hosts():
    hosts = []
    # Generate all possible hostnames
    hosts.extend([f"cpu{str(i).zfill(3)}.cm.cluster" for i in range(1, 3)])
    hosts.extend([f"gpu{str(i).zfill(3)}.cm.cluster" for i in range(1, 23)])
    
    print(f"🔍 Scanning {len(hosts)} hosts...\n")
    
    # Test connections in parallel
    active_hosts = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(test_host, hosts)
        for host in results:
            if host:
                active_hosts.append(host)
    
    return active_hosts

def monitor_host(host):
    try:
        print(f"\n🚀 Starting continuous monitoring on {host}")
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=5672,
                credentials=credentials
            )
        )
        channel = connection.channel()
        channel.exchange_declare(exchange='ufo', exchange_type='fanout')
        queue = channel.queue_declare(queue='', exclusive=True)
        channel.queue_bind(exchange='ufo', queue=queue.method.queue)
        
        def callback(ch, method, properties, body):
            try:
                msg = json.loads(body)
                print(f"\n📡 {datetime.now().isoformat()} [Message on {host}]:")
                print(json.dumps(msg, indent=2))
            except json.JSONDecodeError:
                print(f"\n⚠️ {datetime.now().isoformat()} [Raw message on {host}]:")
                print(body.decode())

        channel.basic_consume(
            queue=queue.method.queue,
            on_message_callback=callback,
            auto_ack=True
        )
        print("✅ Connected and listening (Ctrl+C to stop)...")
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped")
    except Exception as e:
        print(f"❌ Monitoring failed: {str(e)}")
    finally:
        try:
            connection.close()
        except:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto', action='store_true', help='Auto-discover active hosts')
    parser.add_argument('--host', help='Specific host to monitor')
    args = parser.parse_args()
    
    if args.auto:
        active_hosts = find_active_hosts()
        if active_hosts:
            print("\n🎉 Active hosts found:")
            for host in active_hosts:
                print(f"• {host}")
            monitor_host(active_hosts[0])  # Monitor first active host
        else:
            print("\n😢 No active hosts found")
    elif args.host:
        monitor_host(args.host)
    else:
        print("Usage:")
        print("  Auto-discover: python3 rabbitmq_test.py --auto")
        print("  Test specific host: python3 rabbitmq_test.py --host cpu001.cm.cluster")
