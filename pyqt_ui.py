import json
import sys
import threading
import pyqtgraph as pg
from PyQt5 import QtCore, QtWidgets
from kafka import KafkaConsumer, TopicPartition

global prices, short_ma, long_ma, short_ema, long_ema, rsi, timer, notification
prices = [0 for i in range(100)]
short_ma = [0 for i in range(100)]
long_ma = [0 for i in range(100)]
short_ema = [0 for i in range(100)]
long_ema = [0 for i in range(100)]
rsi = [0 for i in range(100)]
timer = [i for i in range(100)]
notification = [""]
analysed_topic = "stock_outputs"

STOCK_PARTITION = {"AAPL": 0, "GOOGL": 1, "AMZN": 2, "MSFT": 3, "TSLA": 4}
stock_symbol = sys.argv[1]
partition = STOCK_PARTITION.get(stock_symbol)

data_consumer = KafkaConsumer(bootstrap_servers="192.168.56.104",
    auto_offset_reset='latest',  # Start consuming from the latest offset
    enable_auto_commit=False,  # Disable automatic committing of offsets
                               )

data_consumer.assign([TopicPartition(analysed_topic, partition)])
data_consumer.seek_to_end(TopicPartition(analysed_topic, partition))


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()

        # Temperature vs time dynamic plot
        self.plot_graph = pg.PlotWidget()
        self.setCentralWidget(self.plot_graph)
        self.plot_graph.setBackground("w")
        price_pen = pg.mkPen(color=(255, 0, 0))
        short_ma_pen = pg.mkPen(color=(0, 255, 0))
        long_ma_pen = pg.mkPen(color=(0, 0, 255))
        short_ema_pen = pg.mkPen(color=(255, 255, 0))
        long_ema_pen = pg.mkPen(color=(255, 0, 255))
        rsi_pen = pg.mkPen(color=(0, 255, 255))
        self.plot_graph.setTitle(f"{stock_symbol} \n {notification}", color="b", size="20pt")
        styles = {"color": "red", "font-size": "18px"}
        self.plot_graph.setLabel("left", "price", **styles)
        self.plot_graph.setLabel("bottom", "price index", **styles)
        self.plot_graph.addLegend()
        self.plot_graph.showGrid(x=True, y=True)
        self.plot_graph.setYRange(930, 1100)
        self.time = list(range(100))
        self.price = [0 for i in range(100)]
        self.short_ma = short_ma
        self.long_ma = long_ma
        self.short_ema = short_ema
        self.long_ema = long_ema
        self.rsi = rsi
        self.notification = notification
        # Get a line reference
        self.price_line = self.plot_graph.plot(
            self.time,
            self.price,
            name="price",
            pen=price_pen,
            symbol="+",
            symbolSize=15,
        )
        self.short_ma_line = self.plot_graph.plot(
            self.time,
            self.short_ma,
            name="short_ma",
            pen=short_ma_pen,
            symbol="+",
            symbolSize=8,
        )
        self.long_ma_line = self.plot_graph.plot(
            self.time,
            self.long_ma,
            name="long ma",
            pen=long_ma_pen,
            symbol="+",
            symbolSize=8,
        )
        self.short_ema_line = self.plot_graph.plot(
            self.time,
            self.short_ema,
            name="short ema",
            pen=short_ema_pen,
            symbol="+",
            symbolSize=8,
        )
        self.long_ema_line = self.plot_graph.plot(
            self.time,
            self.long_ema,
            name="long ema",
            pen=long_ema_pen,
            symbol="+",
            symbolSize=8,
        )
        self.rsi_line = self.plot_graph.plot(
            self.time,
            self.rsi,
            name="rsi",
            pen=rsi_pen,
            symbol="+",
            symbolSize=8,
        )
        # Add a timer to simulate new temperature measurements
        self.timer = QtCore.QTimer()
        self.timer.setInterval(300)
        self.timer.timeout.connect(self.update_plot)
        self.timer.start()

    def update_plot(self):
        self.time = timer
        self.price = prices
        self.short_ma = short_ma
        self.long_ma = long_ma
        self.short_ema = short_ema
        self.long_ema = long_ema
        self.rsi = rsi
        self.notification = notification
        self.price_line.setData(self.time, self.price)
        self.short_ma_line.setData(self.time, self.short_ma)
        self.long_ma_line.setData(self.time, self.long_ma)
        self.short_ema_line.setData(self.time, self.short_ema)
        self.long_ema_line.setData(self.time, self.long_ema)
        self.rsi_line.setData(self.time, self.rsi)
        self.plot_graph.setTitle(f"{stock_symbol} \n {self.notification[-1]}", color="b", size="20pt")

def show_ui():
    app = QtWidgets.QApplication([])
    main = MainWindow()
    main.show()
    app.exec()

def fill_data():
    global notification
    print("in fill data")
    for message in data_consumer:
        print(message)
        dict_data = json.loads(message.value)
        new_price, new_short_ma, new_long_ma, new_short_ema, new_long_ema, new_rsi, new_signal = \
            dict_data["closing_price"], dict_data["short_MA"], dict_data["long_MA"], dict_data["short_EMA"],\
            dict_data["long_EMA"], dict_data["rsi"], dict_data["signal"]
        timer.pop(0)
        prices.pop(0)
        short_ma.pop(0)
        long_ma.pop(0)
        short_ema.pop(0)
        long_ema.pop(0)
        rsi.pop(0)
        prices.append(new_price), short_ma.append(new_short_ma), long_ma.append(new_long_ma), short_ema.append(new_short_ema), long_ema.append(new_long_ema), rsi.append(new_rsi)
        print(new_signal)
        timer.append(timer[-1] + 1)
        if new_signal == "buy" or new_signal == "sell":
            print("if done")
            notification = [new_signal]
        print(notification)




thread_show_ui = threading.Thread(target=show_ui)
thread_fill_data = threading.Thread(target=fill_data)
thread_show_ui.start()
thread_fill_data.start()

thread_show_ui.join()
thread_fill_data.join()