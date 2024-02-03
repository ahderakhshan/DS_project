from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import sys
import pandas as pd
import json
# Kafka broker and topic information
bootstrap_servers = "192.168.56.104:9092"
prices_topic = "stock_prices"
analysed_topic = "stock_outputs"
# Global variables
SHORT_PERIOD = 20
LONG_PERIOD = 50
STOCK_PARTITION = {"AAPL": 0, "GOOGL": 1, "AMZN": 2, "MSFT": 3, "TSLA": 4}
stock_symbol = sys.argv[1]
selected_partition = STOCK_PARTITION.get(stock_symbol)
in_buy = False


def calculate_avg(prices):
    return sum(prices) / len(prices) if len(prices) != 0 else 0


def calculate_ema(prices):
    column = ['Price']
    df = pd.DataFrame(prices, columns=column)
    result = df.ewm(com=0.8).mean().iloc[-1]['Price']
    return result


def calculate_rsi(prices):
    price_diffs = [prices[i+1] - prices[i] for i in range(len(prices) - 1)]
    gain, loss = [], []
    for i in price_diffs:
        if i > 0:
            gain.append(i)
        else:
            loss.append(-1*i)
    gain_avg, loss_avg = calculate_avg(gain), calculate_avg(loss)
    rs = gain_avg / loss_avg if loss_avg != 0 else 10000000
    rsi = 100 - (100 / (1 + rs))
    return rsi


def is_buy(current_short_mean, current_long_mean, last_short_mean, last_long_mean):
    return current_short_mean > current_long_mean and last_long_mean > last_short_mean


def is_sell(current_long_mean, current_short_mean, last_short_mean, last_long_mean):
    return current_long_mean > current_short_mean and last_short_mean > last_long_mean


# Create a Kafka consumer
price_consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',  # Start consuming from the latest offset
    enable_auto_commit=False,  # Disable automatic committing of offsets
)
analysed_producer = KafkaProducer(
    bootstrap_servers='192.168.56.104:9092'
)
price_consumer.assign([TopicPartition(prices_topic, selected_partition)])
price_consumer.seek_to_end(TopicPartition(prices_topic, selected_partition))
# define list of last items
short_data = [1000.0 for _ in range(SHORT_PERIOD)]
long_data = [1000.0 for _ in range(LONG_PERIOD)]
last_short_mean = 0
last_long_mean = 0
current_short_mean = 0
current_long_mean = 0
# start consuming messages
try:
    # Continuously consume messages
    for message in price_consumer:
        print(message)
        last_long_mean = current_long_mean
        last_short_mean = current_short_mean
        price = float(message.value.decode('utf-8'))
        short_data.pop(0)
        long_data.pop(0)
        short_data.append(price)
        long_data.append(price)
        current_short_mean, current_long_mean = calculate_avg(short_data), calculate_avg(long_data)
        current_short_ema, current_long_ema = calculate_ema(short_data), calculate_ema(long_data)
        rsi = calculate_rsi(long_data)
        trade_signal = "no action"
        if is_buy(current_short_mean, current_long_mean, last_short_mean, last_long_mean): # and current_short_ema > current_long_ema and rsi < 30:
            # generate buy signal
            trade_signal = "buy"
        if is_sell(current_long_mean, current_short_mean, last_short_mean, last_long_mean): # and current_short_ema < current_long_ema and rsi > 70:
            # generate sell signal
            trade_signal = "sell"
        analysed_data = {
            "closing_price": price,
            "short_MA": current_short_mean,
            "long_MA": current_long_mean,
            "short_EMA": current_short_ema,
            "long_EMA": current_long_ema,
            "rsi": rsi,
            "signal": trade_signal
        }
        analysed_producer.send(topic=analysed_topic, key=stock_symbol.encode('utf-8'), value=json.dumps(analysed_data).encode('utf-8'),
                      partition=STOCK_PARTITION.get(stock_symbol))


except KeyboardInterrupt:
    # Handle keyboard interrupt (Ctrl+C) to gracefully exit
    pass

finally:
    # Close the consumer when done (this won't be executed in an infinite loop)
    price_consumer.close()


if __name__ == '__main__':
    pass
