# promotions-engine


Getting the Avro schema registry to run locally is quite complex if you don’t take the Confluent pre built option
Note that you could also just run it using Docker. 

You need to build Kafka first by doing:

 * First of all you need to build kafka as there are dependencies on this on the other modules like common
 * Go to https://github.com/confluentinc/kafka and clone
     *  Then run ./gradlew install (gradlew is install by running gradle)
       *  note gradleAll install which I think does it for multiple scala versions got stuck
 *  With Kafka installed into your local maven repo you’ll have dependencies needed by commons
 *  Get common here https://github.com/confluentinc/common and git clone then mvn install
 *  Get rest utils here https://github.com/confluentinc/rest-utils and git clone then mvn install
 *  SchemaRegistry is here https://github.com/confluentinc/schema-registry
   *  do a mvn package for this one
 *  Once done copy config/schema-registry.properties somewhere - note there is one now in the repo that you can use in dev_utility_scripts
 *  and run ./bin/schema-registry-start schema-registry.properties

#### Notes around building
I had one occasion when I found that the rest-utils wanted Kafka under scala 1.12 but I had only built 1.13

At first 1.12 kafka wouldn't build but after an update it did. Note I did installAll to get both 1.12 and 1.13 installed in .m2 

As of 23rd May common wouldn't build with an error about a package being too big. Had to add 
<configuration><tarLongFileMode>posix</tarLongFileMode></configuration> to the pom.xml in the 
package dir 

## Util Scripts
The scripts in the dev_utility_scripts file expect certain environment vars to be present
I made a file to source these called personal_vars (which is in .gitignore)
 * export MY_KAFKA=/Users/dho15/Code/external/kafka_repos/kafka
 * export MY_SCHEMA_REG=/Users/dho15/Code/external/kafka_repos/kafka
 * export PATH=${PATH}:/Users/dho15/Documents/tools/sbt_1310/bin

## List of commands for initial setup
 * git clone git@github.com:confluentinc/kafka.git
 * git clone git@github.com:confluentinc/rest-utils.git
 * git clone git@github.com:confluentinc/common.git
 * git clone git@github.com:confluentinc/schema-registry.git

 * cd kafka
 * ./gradlew install
 * cd ../common
 * /usr/local/apache-maven-3.6.3/bin/mvn install
 * cd ../rest-utils
 * /usr/local/apache-maven-3.6.3/bin/mvn install
 * cd ../schema-registry
 * /usr/local/apache-maven-3.6.3/bin/mvn package
 
 * Set up the personal_vars file or
   * export MY_KAFKA=/usr/local/kafka_2.12-2.3.0
   * export MY_SCHEMA_REG=/usr/local/schema-registry/bin/schema-registry-start
 * pip install "confluent-kafka[avro]"
   * This had to be run on bash, wasn't working on zsh weirdly
 * ./dev_utility_scripts/start_local_kafka.sh
 * ./dev_utility_scripts/start_local_schema_registry.sh
   * Run after the kafka one as it needs zookeeper
   
   
Note paths may need updating dependent on mvn location/ if it is on PATH

# Running and compiling the applications
There are two apps, the simpler ims one and the more complex promotions/rewards one
To compile or run either start sbt in the root dir then do:
 * ims/compile or ims/run
 * promotions/compile or promotions/run
 
On the Python side a venv should be setup and activated before doing:
 * cd py_producers_consumers
 * pip install -r requirements.txt

Consumers and producers for the ims and promotions apps can then be run like:
 * python <file>.py e.g. python ims/producer.py or ims/readTerritoryQueues.py
 
 Note you'll need python8 upwards to get the right avro libraries 