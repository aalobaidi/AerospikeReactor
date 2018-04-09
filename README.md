[![Build Status](https://travis-ci.org/aalobaidi/AerospikeReactor.svg?branch=master)](https://travis-ci.org/aalobaidi/AerospikeReactor)
[![codecov](https://codecov.io/gh/aalobaidi/AerospikeReactor/branch/master/graph/badge.svg)](https://codecov.io/gh/aalobaidi/AerospikeReactor)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.aalobaidi/aerospike-reactor/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.aalobaidi/aerospike-reactor)
# Aerospike Reactor
AerospikeReactor is a reactive wrapper for Aerospike Java Client based on [Project Reactor](https://projectreactor.io/) and compatible with [Spring Framework 5](https://spring.io/) and [Spring Webflux](https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html).

AerospikeReactor is written in Java, fully compatible with Java 8, 9 and 10. It also has first class support for Kotlin.
   
## Features
1. Fully non-blocking and asynchronous. 
2. Compatible with Java 8, 9 and 10.
3. Full support for Kotlin.
4. Efficient use of threads with event loops (supports Netty event loops).
5. Netty event loops could be shared with [Spring Webflux](https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html).
## Maven Central
AerospikeReactor is hosted in Maven Central. To add the library to your project:
1. Gradle
```
compile("com.github.aalobaidi:aerospike-reactor:0.1")
```
2. Maven
```xml
<dependency>
    <groupId>com.github.aalobaidi</groupId>
    <artifactId>aerospike-reactor</artifactId>
    <version>0.1</version>
</dependency>
```
## Examples
To run the sample Spring Boot application, you need Docker installed.  
```bash
cd examples-kotlin
# run Aerospike with Docker
docker run -tid --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike/aerospike-server

# run the Spring Boot Sample
./gradlew clean bootRun
```
