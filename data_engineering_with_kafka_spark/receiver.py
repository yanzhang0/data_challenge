from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer, KafkaClient
import json



def get_dic(rdd_list):
    """
    function to turn aggregated data into dict
    """
    res = []
    for elm in rdd_list:
        tmp = {elm[0]: elm[1]}
        res.append(tmp)
    return json.dumps(res)


def sendmsg(rdd):
    """
    function to send data back to kafka 
    """
    if rdd.count != 0:
        msg = get_dic(rdd.collect())
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.send("result", msg.encode('utf8'))
        producer.flush()


def start():
    i=0
    sconf=SparkConf()
    sconf.set('spark.cores.max' , 2)
    sc=SparkContext(appName='test',conf=sconf)
    
    interval=30
    ssc=StreamingContext(sc,interval)

    brokers="localhost:9092"
    topic='test-topic'
    kafkaStreams = KafkaUtils.createDirectStream(ssc,[topic],kafkaParams={"metadata.broker.list": brokers})
     
    parsed = kafkaStreams.map(lambda v: json.loads(v[1]))

    #aggregating data at each interval of 30 seconds
    tech_dstream = parsed.map(lambda line: line['tkNameIdProvider'])
    tech_count=tech_dstream.countByValue()
    tech_count.pprint()


    #send data back to kafka 
    tech_count.foreachRDD(lambda x : sendmsg(x))



    kafkaStreams.transform(storeOffsetRanges)

    #kafkaStreams.pprint()
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate

offsetRanges = []


def printOffsetRanges(rdd):
    """
    function only used for debuging
    """
    for o in offsetRanges:
        print ("%s %s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset,o.untilOffset-o.fromOffset))


def storeOffsetRanges(rdd):
    """
    function to set range of data
    """
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

if __name__ == '__main__':
    start()


