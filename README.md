[![Build Status](https://app.travis-ci.com/agebe/kvd.svg?branch=main)](https://app.travis-ci.com/github/agebe/kvd/builds)

# kvd

kvd is a simple streaming key value client/server database with support for large values. kvd supports following operations:

* **put**, add/replace a key/value pair
* **get**, get a value
* **contains**, check if a key exists
* **remove**, remove a key/value pair

See example code below.

Features:
* values are streamed in/out of the database (tested with single values of multiple GiB in size)
* transaction support, optionally with optimistic/pessimistic concurrency control
* optional key expiry and removal, either after write (create) or last access, or both.

kvd client and server are written in Java.

I've written kvd to cache calculation results that take quite some time to compute. The results are sometimes large (> 1GB) and it seems key value databases struggle with large values so I've rolled my own.

**Note: I've only tested kvd on linux/x86_64. It might work on other OS/arch combinations.**

## Server setup

kvd can be started with --help that prints out all options.

```
Usage: kvd [options]
  Options:
    --blob-split-size
      split blob files when they reach this size. Unit can be specified 
      (k,kb,ki,m,mb,mi,g,gb,gi,t,tb,ti). 
      Default: 16ti
    --blob-threshold
      store values external to MapDB when they reach this size. Unit can be 
      specified (k,kb,ki,m,mb,mi,g,gb,gi,t,tb,ti)
      Default: 64ki
    --client-timeout
      client timeout. Unit can be specified ms, s, m, h, d, defaults to 
      seconds. 
      Default: 1m
    --concurrency-control, -cc
      default concurrency control, options: NONE, optimistic (non-blocking, 
      OPTW or OPTRW), pessimistic (blocking, PESW or PESRW)
      Default: NONE
      Possible Values: [NONE, OPTW, OPTRW, PESW, PESRW]
    --datadir
      path to data directory
      Default: $HOME/.kvd
    --default-db-name
      name of the default database
      Default: default
    --disable-deadlock-detector
      disable thread deadlock detector
      Default: false
    --expire-after-access
      removes entries from the database after no access within this fixed 
      duration. Defaults to never expire. Duration unit can be specified ms, 
      s, m, h, d, defaults to seconds.
    --expire-after-write
      removes entries from the database after creation  fixed duration. 
      Defaults to never expire. Duration unit can be specified ms, s, m, h, d, 
      defaults to seconds.
    --expire-check-interval
      how often to check for expired keys. Unit can be specified ms, s, m, h, 
      d, defaults to seconds.
    --help
      show usage
    --log-expired
      info log expired keys
      Default: false
    --log-level
      logback log level (trace, debug, info, warn, error, all, off). Configure 
      multiple loggers separated by comma
      Default: root:info
    --max-clients
      maximum number of clients that can connect to the server at the same 
      time 
      Default: 100
    --port
      port to listen on
      Default: 3030
    --socket-so-timeout
      server socket so timeout. Unit can be specified ms, s, m, h, d, defaults 
      to seconds.
      Default: 1m
```

### Docker

To start a test server for playing do this:
```bash
$ docker run --rm -ti --name kvd -p 3030:3030 agebe/kvd:0.6.0
```

You might want to keep the database files between restarts or change some JVM settings etc. do this:
```bash
$ docker run --rm --name kvd -ti -v myvolume:/datadir -p 3030:3030 -e JAVA_OPTS="-verbose:gc" agebe/kvd:0.6.0 --datadir /datadir
```

### Running the server from source

* Clone kvd from this repository
* Make sure you have jdk 11+ installed.
* Install recent version of [gradle](https://gradle.org/releases/)
* Change dir into ./kvd-server and execute:
```bash
$ gradle run
```
In this case the database is written to $HOME/.kvd

## Client examples

For the example below to work you need to add the kvd-client library as a dependency in your project. The kvd-client library depends on slf4j and java 1.8+.

Gradle:
```gradle
dependencies {
  implementation 'io.github.agebe:kvd-client:0.6.0'
}
```

Maven:
```xml
<dependency>
  <groupId>io.github.agebe</groupId>
  <artifactId>kvd-client</artifactId>
  <version>0.6.0</version>
</dependency>
```

The examples below show how to use the client API to access the database

### Simple example
```java
    try(KvdClient client = new KvdClient("localhost:3030")) {
      client.putString("my-key", "my-value");
      System.out.println(client.getString("my-key"));
    }
```

### Stream example
```java
    try(KvdClient client = new KvdClient("localhost:3030")) {
      try(DataOutputStream out = new DataOutputStream(client.put("simplestream"))) {
        out.writeLong(42);
      }
      InputStream i = client.get("simplestream");
      if(i != null) {
        try(DataInputStream in = new DataInputStream(i)) {
          System.out.println(in.readLong());
        }
      } else {
        throw new RuntimeException("value missing");
      }
    }
```

kvd does not bring its own serialization support but put- and get-operations are based on Java IO streams so integration with your serialization system should be straightforward ([json example](https://github.com/agebe/kvd/blob/main/kvd-server/src/test/java/kvd/test/JsonTest.java))

[KvdClient API Javadoc](https://javadoc.io/doc/io.github.agebe/kvd-client/latest/kvd/client/KvdClient.html)

[![javadoc](https://javadoc.io/badge2/io.github.agebe/kvd-client/javadoc.svg)](https://javadoc.io/doc/io.github.agebe/kvd-client)

## Storage Backend
kvd uses [MapDB](https://mapdb.org/) to store key/values. Values are either stored inline in MapDB or as an external file depending on size (--blob-threshold CLI option).

## Future work
* configurable max size for single values
* configurable max size for database. This might automatically drop values to make room for new values (LRU)
* client/server network transport encryption
* client/server authentication
