from kafka import KafkaConsumer, KafkaProducer
import certifi
import time

__REGION='uk-london-1'
__ORIGIN_STREAM='send'
__DESTINATION_STREAM='rec'
__TENANCY_NAME='ecrcloud'
__STREAMPOOL_OCID='ocid1.streampool.oc1.uk-london-1.amaaaaaatwfhi7yalnuxxqgad2opy5kainapshftx5kurpjfjzcyacs5e4nq'
__OCI_USERNAME='oracleidentitycloudservice/matthew.mcloughlin@oracle.com'
__SASL_USERNAME=__TENANCY_NAME+'/'+__OCI_USERNAME+'/'+__STREAMPOOL_OCID
__SASL_TOKEN='jsvT{{{vJ0.WA3M3Wk;b'
__BOOTSTRAP_SERVER='cell-1.streaming.'+__REGION+'.oci.oraclecloud.com'

# print(__SASL_USERNAME)


def oci_consumer():
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

    consumer.subscribe([__ORIGIN_STREAM])

    data = consumer.poll(1000)

    consumer.commit()
    consumer.close()
    after_consume = time.time()
    print("Consume time: " + str(after_consume - before_consume))

    return data


def oci_producer(data):
    before_produce = time.time()
    producer = KafkaProducer(
        bootstrap_servers=__BOOTSTRAP_SERVER,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=__SASL_USERNAME,
        sasl_plain_password=__SASL_TOKEN,
        ssl_certfile=certifi.where(),
    )

    for k, v in data.items():
        for item in v:
            to_send = b'Recieved: '+str(item.value).encode('ascii')
            print(to_send)
            producer.send(__DESTINATION_STREAM, to_send)

    after_produce = time.time()

    print("Produce time: " + str(after_produce - before_produce))

    return


if __name__ == "__main__":
    data = oci_consumer()
    oci_producer(data)
