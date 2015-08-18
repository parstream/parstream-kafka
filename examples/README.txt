This ParStream Kafka Consumer can be run to read Avro or JSON data from 
a running Kafka server, decode the messages into a corresponding ParStream 
format, and stream them into a ParStream database.


Steps to run:

1- Compile the base ParStream Kafka Consumer and the used Kafka Message Decoder:
   $ mvn install -f consumer/pom.xml
   $ mvn install -f decoder-json/pom.xml

2- To produce an executable JAR example:
   $ mvn install -f examples/example-json/pom.xml

4- Assuming ParStream and Kafka servers are running:

   4.1- Step into the example folder
        $ cd examples/example-json

   4.2- Export LD_LIBRARY_PATH
        $ export LD_LIBRARY_PATH=<PATH_TO_PARSTREAM_INSTALLATION>/lib

   4.3- Adapt the configuration files (consumer.properties, database.properties, mapping.ini),
        to match the configuration of your running ParStream and Kafka instances

   4.4- Execute the jar with the configuration files in the current directory
        $ java -jar target/kafka-example-json-<VERSION>.jar

Note: To use the Avro decoder instead of the JSON decoder, replace the 'json'
      instances in the above instruction set to 'avro'
