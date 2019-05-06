Запускаем Zookeeper
    bin\windows\zookeeper-server-start.bat config\zookeeper.properties
Запускаем Kafka1:
    bin\windows\kafka-server-start.bat config\kafka1\server.properties
Запускаем Kafka2:
    bin\windows\kafka-server-start.bat config\kafka2\server.properties


bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic messages
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic delivery-output-stream
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic delivery-result-stream


bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic processedMessages


bin\windows\kafka-console-producer --broker-list localhost:9092 --topic messages
bin\windows\kafka-console-consumer --bootstrap-server localhost:9092 --topic messagesProcessed --from-beginning
bin\windows\kafka-console-consumer --bootstrap-server localhost:9092 --topic log-output-stream --from-beginning



bin\windows\kafka-console-producer --broker-list localhost:9092 --topic delivery-output-stream
bin\windows\kafka-console-consumer --bootstrap-server localhost:9092 --topic delivery-output-stream --from-beginning
bin\windows\kafka-console-consumer --bootstrap-server localhost:9092 --topic delivery-result-stream --from-beginning



{"message":"OK"}


{"queue":"start_queue","context":"<Context/>","time":"2001-07-04T12:08:56"}
{"uuid":"8d1aaa2b-daa5-453a-91dc-69394d3ca8fc","queue":"start_queue","context":"Context","time":"2019-04-29T15:46:46"}
