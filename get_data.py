from flask import Flask, request
import time

app = Flask(__name__)

@app.route('/ingest', methods=['POST'])
def ingest():
    data = request.get_json()
    print(f"Received data: {data}")
    # You can add your processing logic here
    time.sleep(10)
    return {'status': 'success'}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
