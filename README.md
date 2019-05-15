# data_challenge
playground for data engineering

all text files are stored in the subfolder "./events"


# In addition to the standalone version with sql, a more complicated version with spark and kafka is provided. 

Files in the folder data_engineering_with_kafka_spark together simulate a process to use kafka and spark to process data. 

In three terminals, run the following lines separately:
Terminal 1:
python producer.py

Terminal 2:
spark-submit --jars spark-streaming-kafka.jar receiver2.py

Terminal 3:
python consumer.py 


Terminal 1 turns the json file into streaming data
Terminal 2 processes the streaming data from kafka using spark and sends it back to kafka
Terminal 3 shows data aggregated by minute from kafka stream
