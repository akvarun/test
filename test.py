import requests
import json

def test_servers():
    for i in range(1, 23):
        server = f"gpu{str(i).zfill(3)}.cm.cluster:65535"
        try:
            response = requests.post(f"http://{server}/resolve_coref",
                                     headers={"Content-Type": "application/json"},
                                     data=json.dumps({"text": "John said he would help Mary. She was grateful."}),
                                     timeout=3)
            if response.status_code == 200:
                print(f"[+] Server {server} is live.")
                print(response.json())
                break
        except requests.RequestException as e:
            print(f"[-] Server {server} failed: {e}")

test_servers()
