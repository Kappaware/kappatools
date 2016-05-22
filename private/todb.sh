

set -x
cd k2jdbc
gradle fatJar; java -jar build/libs/k2jdbc-0.6.2-SNAPSHOT.jar --consumerGroup test1b --clientId client2  --adminEndpoint "20166" \
--brokers "wn1.kfk2:9092,wn2.kfk2:9092,wn3.kfk2:9092" --topic logs2-topic1  \
--jdbcUrl jdbc:postgresql://localhost:5432/kappatools --dbUser postgres --dbPassword Netbios --table logs2 \
--colMapping "src_timestamp=otimestamp,in_timestamp=kfkey_extts_timestamp,message=data,remote=kfkey_partitionKey,gate_id=kfkey_extts_gateid,src_counter=count,in_counter=kfkey_extts_counter,key_counter=kfkey_keycounter" \
--samplingPeriod 1000 \
--statson
 