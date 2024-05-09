**Distributed System Final Project**
This project has been implemented as the final project of the distributed systems course in the fall semester of 1402.
In this project, a generator starts to produce data in the form of financial data, and the main task of the students was to implement a scalable system to read and analyze
this data that was produced for different stocks. For this purpose, the data was first read by a Flask code and sent to a partition of a Kafka topic according to the stock name.
In the next step, a program is run for each partition to calculate different indicators such as average, weighted average, and RSA, then in the next step,
each indicator is sent to a partition of another Kafka topic, and finally, the user interface that is implemented with PYQT helps the end user in transactions
by displaying prices and criteria and buying or selling signals.
