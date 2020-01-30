# Apache Flink Examples
This is the code repository for the Streaming ETL examples using Apache Flink. My blog on dzone refers to these examples. This project will be updated with new examples. For official Flink documentation please visit [https://flink.apache.org/](https://flink.apache.org/)

### Package - org.pd.streaming.window.example
* It contains an example of using TumblingWindow to sum integers. 
* A simple IntegerGenerator class is used as source to produce a number every second.
* The example inlcudes TimeWindow with ProcessFunction and CountWindow with ReduceFunction

### Package - org.pd.streaming.aggregation.simple
* It contains simple aggregation logic for Integers and recommended as starting point for beginners. A simple source class which emits 10 continiously increasing integers every second as default.
* This source is then passed to IntegerSum class which creates a StreamingExecutionEnvironment, a data stream and finally executes the environment to start the streaming computation.
* As long as the process is running, it will keep on printing aggregated value of all integers collected by Flink every 5 seconds tumbling window. A tumbling window is very easy to understand is one of many window supported by Flink.
* IntegerSumWithReduce class uses reduce() instead of apply() method to demo the incremental computation feature of Flink.

### Package - org.pd.streaming.aggregation.key
* It contains classes which demo usage of a keyed data stream. Every integer is emitted with a key and passed to Flink using two options: Flink Tuple2 class and a Java POJO.
* The logic is same (compute sum of all integers), however we tell Flink to find a key at an index (Tuple2) or use a getter (POJO). IntegerSumWithKey class uses Tuple2 and IntegerSumWithKeyFromPojo uses a Java POJO class called MyData

### Package - org.pd.streaming.event
* It contains classes which demo usage of Flink CEP API for Event processing. A event here is instance of SensorEvent class which
simulates simple state events from sensors. 
* EventAnalysisCEP class declares a Pattern to identify if a connected_event is immediately followed by disconnected_event within 5 seconds. This pattern is used to create Flink PatternStream which prints matching event if they reach certain threshold.

### Package - org.pd.streaming.application.queue
* It contains classes which demo usage of using ActiveMQ as source. It uses Spring Boot's default in-memory activemq support
and exposes a REST endpoint POST http://localhost:8080/logs to accept messages.
* These messages will be send to queue and Flink will consume it as and when it arrives allowing loose coupling.

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

(For testing Flink with ActiveMQ example, start application as follows, and POST apache server log message in JSON to http://localhost:8080/logs)
```
java -jar flink-tutorials-0.0.1-SNAPSHOT.jar
```
Sample log data in JSON
```
{
  "ident": "-",
  "auth": "-",
  "message": "127.0.0.1 - - [19/Dec/2019:10:50:21 +0000] \"GET /wp-content HTTP/1.0\" 200 5036 \"http://www.fakelog.com\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5321 (KHTML, like Gecko) Chrome/15.0.837.0 Safari/5321\"",
  "response": "200",
  "timestamp": "19/Dec/2019:10:50:21 +0000",	
  "httpversion": "1.0",
  "referrer": "\"http://www.fakelog.com\"",
  "agent": "\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/5321 (KHTML, like Gecko) Chrome/15.0.837.0 Safari/5321\"",
  "verb": "GET",
  "request": "/wp-content",
  "bytes": "5036",
  "clientip": "127.0.0.1"
}
```

