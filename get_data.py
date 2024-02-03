from xml.sax.saxutils import prepare_input_source

from flask import Flask, request
from kafka import KafkaProducer, KafkaConsumer, KafkaClient
import time

TOPIC_NAME = 'stock_prices'
STOCK_PARTITION = {"AAPL": 0, "GOOGL": 1, "AMZN":  2, "MSFT": 3, "TSLA": 4}
app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers='192.168.56.104:9092'
)


@app.route('/ingest', methods=['POST'])
def ingest():
    data = request.get_json()
    symbol = data.get('stock_symbol')
    print(type(symbol))
    if symbol:
        close_price = data.get('closing_price')
        if close_price:
            close_price = str(close_price)
            producer.send(topic=TOPIC_NAME, key=symbol.encode('utf-8'), value=close_price.encode('utf-8'),
                          partition=STOCK_PARTITION.get(symbol))
            print(f"Received data: {data}")
    return {'status': 'success'}, 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
