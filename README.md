# Apache Flink Examples
This is the code repository for the Streaming ETL examples using Apache Flink. My blog on dzone refers to these examples. This project will be updated with new examples. For official Flink documentation please visit [https://flink.apache.org/](https://flink.apache.org/)

### Package - org.pd.streaming.aggregation.simple
* It contains simple aggregation logic for Integers and recommended as starting point for beginners. A simple source class which emits 10 continiously increasing integers every second as default.
* This source is then passed to IntegerSum class which creates a StreamingExecutionEnvironment, a data stream and finally executes the environment to start the streaming computation.
* As long as the process is running, it will keep on printing aggregated value of all integers collected by Flink every 5 seconds tumbling window. A tumbling window is very easy to understand is one of many window supported by Flink.

## Building this project from Source
Prerequisites:
* Git
* Maven
* Java 8+

```
git clone https://github.com/preetdeepkumar/flink-tutorials.git
cd flink-tutorials
mvn clean install -DskipTests
```

It will create a single executable fat jar. Running the jar will keep on printing values to console till the process is running.
```
java -jar flink-tutorials-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```
Alternately, using your IDE directly run main class in org.pd.streaming.application.Main


