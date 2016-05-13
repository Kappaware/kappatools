# kappatools

Kappatooks is a small set of java tools to exercices Kafka new Consumer and producer API.

Intended usage are: 

- A set of learning tools to experiment about new Kafka api behavior

- A test system, to exercise Kafka infrastructure at scale

- A code base for new, more sophisticated domain specific application.

Kappatools is made of three tools:

- kgen, which is a event generator, for test ingestion.
- kcons, which is a message consumer, as kafka original kafka-console-consumer.sh.
- k2k, which is a kafka to kafka event copier.


## License

    Copyright (C) 2015 BROADSoftware

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
	
## Usage

All tools are provided as 'fatJar', which are jar embedding all dependencies. Of course, this is not the most effective if term of file size, but avoid a lot of administrative burden (No more classpath problem, not jar version mismatch...)



## Parameters

brokers
topic
properties
forceProperties
consumerGroup
clientId

samplingPeriod (*)
statson (*)
messon (*)

adminEndpoint
adminAllowedNetworks

#kgen specific
burstCount (*)
gateId
period (*)
initialCounter



