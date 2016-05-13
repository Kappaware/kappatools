
# Set JAVA_HOME. Must be at least 1.7.
# If not set, will try to lookup a correct version.
# JAVA_HOME=/some/place/where/to/find/java

# Set the log configuration file
JOPTS="$JOPTS -Dlog4j.configuration=file:/etc/king/log4j.xml"

# ---------------------------------------------------- Mandatory parameters

# Set kafka bootstrap server(s)
# Define the kafka cluster to store incoming message
# OPTS="$OPTS --brokers srv1:9092,srv2:9092,srv3:9092"

# Set gate ID (Must be globally unique)
# This will be used to tag every incoming message, as part of extTs (Extended Timestamp)
# OPTS="$OPTS --gateId gate1"

# Set target topic
# OPTS="$OPTS --topic wharf1"

# ------------------------------------------------------ Optional parameters
# Uncomment to activate. Default value are provided as sample.

# Set keyLevel
# 1, 2 or 3. Define the richness of information included in the key. From the received HTTP request.
# 1: extTs (Timestamp + gateId + counter + partitionKey (= remote (sender))) IP address
# 2: 1 + verb (PUT,POST,GET...) + characterEncoding + contentType + pathInfo + contentLength
# 3: 2 + protocol + scheme + serverName + serverPort + header + query parameters.
#
# OPTS="$OPTS --keyLevel 1"

# Set endoint. Listening address for king server.
# Allow to bind on a specific interface, or on all (0.0.0.0).
# Provide also listening port.
#
# OPTS="$OPTS --endpoint 0.0.0.0:7070"

# Allow to restrict range of IP allowed to send message to king.
# Default is to allow everything: 
# OPTS="$OPTS --allowedNetworks 0.0.0.0/0"

# Several subnet can be provided. For example, to allow all private (RFC1918) networks;
# OPTS="$OPTS --allowedNetworks '10.0.0.0/8,172.16.0.0/12,192.168.0.0/16'"

# Refer to Kafka Producer config for client.id parameter. If not specified, default to --gateId value
# OPTS="$OPTS --clientId wharf1"

# Allow setting of some Kafka producer properties.
# Refer to Kafka Producer config for a list if such properties
# OPTS="$OPTS --properties 'prop1=val1,prop2=val2'"

# king check properties name against a list of valid ones.
# This will allow to skip this test, for example, in case of added properties in a new Kafka API version.
# Use only if you know what you are doing
# OPTS="$OPTS --forceProperties"

# Optional: Set an endoint for a small admin server.
# Allow to bind on a specific interface, or on all (0.0.0.0).
# Provide also listening port.
#
# OPTS="$OPTS --adminEndpoint 0.0.0.0:7070"

# Allow to restrict range of IP allowed to access this admin server.
# Default is to allow only from localhost: 
# OPTS="$OPTS --dminAllowedNetworks 127.0.0.1/32"

# Define the sampling period for throughput statistics evaluation.
# May be modified through the REST admin interface
# OPTS="$OPTS --samplePeriod 1000"

# Allow INFO level messages generation of throughput statistics, on sample period.
# May be set/unset through the REST admin interface
# OPTS="$OPTS --statson"

# Allow INFO level messages generation of every single message (received payload).
# May be set/unset through the REST admin interface
# Use with care, only when receiving low frequency text payload
# OPTS="$OPTS --messon"







