
set -x
cd k2k
gradle fatJar; java -jar build/libs/k2k-0.6.2-SNAPSHOT.jar --consumerGroup logs2 --clientId logs2-k2k --adminEndpoint "20165" \
--sourceBrokers "br1.kfk1.bsa.broadsoftware.com:9092,br2.kfk1.bsa.broadsoftware.com:9092,br3.kfk1.bsa.broadsoftware.com:9092" --sourceTopic logt1-entry \
--targetBrokers "wn1.kfk2:9092,wn2.kfk2:9092,wn3.kfk2:9092" --targetTopic logs2-topic1  \
--statson 
