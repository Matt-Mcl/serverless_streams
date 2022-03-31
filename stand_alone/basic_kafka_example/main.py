from kafka import KafkaConsumer, KafkaProducer
import certifi
from datetime import datetime
import time

__REGION='uk-london-1'
__STREAM_NAME='send4'
__TENANCY_NAME='ecrcloud'
__STREAMPOOL_OCID='ocid1.streampool.oc1.uk-london-1.amaaaaaatwfhi7yalnuxxqgad2opy5kainapshftx5kurpjfjzcyacs5e4nq'
__OCI_USERNAME='oracleidentitycloudservice/matthew.mcloughlin@oracle.com'
__SASL_USERNAME=__TENANCY_NAME+'/'+__OCI_USERNAME+'/'+__STREAMPOOL_OCID
__SASL_TOKEN='jsvT{{{vJ0.WA3M3Wk;b'
__BOOTSTRAP_SERVER='cell-1.streaming.'+__REGION+'.oci.oraclecloud.com'

# print(__SASL_USERNAME)

before_produce = time.time()
producer = KafkaProducer(
    bootstrap_servers=__BOOTSTRAP_SERVER,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username=__SASL_USERNAME,
    sasl_plain_password=__SASL_TOKEN,
    ssl_certfile=certifi.where(),
)


future = producer.send(__STREAM_NAME, b'some_message_bytes: '+str(datetime.now()).encode('ascii'))
after_produce = time.time()

before_consume = time.time()
consumer = KafkaConsumer(
    bootstrap_servers=__BOOTSTRAP_SERVER,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username=__SASL_USERNAME,
    sasl_plain_password=__SASL_TOKEN,
    ssl_certfile=certifi.where(),
    group_id="my_group",
)

consumer.subscribe([__STREAM_NAME])

data = consumer.poll(1000)

for k, v in data.items():
    print(v[0].value)

consumer.commit()
consumer.close()
after_consume = time.time()


print("Produce time: " + str(after_produce - before_produce))
print("Consume time: " + str(after_consume - before_consume))
