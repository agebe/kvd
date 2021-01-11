# kvd

kvd is a simple key value database that follows a client/server model. Key features of kvd are:

* Values are streamed in/out which supports large values.
* Keys/Values are stored to the filesystem (no jvm in-memory storage currently), each pair in a separate file.

kvd client and server are written in Java.

Note: I've only tested kvd on linux/x86_64. It might also work on other OS/arch.

I've written kvd to cache calculation results that take quite some time to compute. The results are sometimes large (> 1GB) and it seems other key value database struggle with large values this is why I've decided to roll my own. Currently the kvd server stores each key and value in a separate file so if you are planning to use it for lots of small key/value pairs you might want to consider this and check if your filesystem supports it (e.g. check free inodes).

## Server setup

kvd can be started with --help that prints out all options.

The server listens by default on TCP port 3030 and writes to $HOME/.kvd

### Docker

To start a a test server for playing do this:
```bash
$ docker run --rm -ti --name kvd -p 3030:3030 agebe/kvd:0.0.2
```

Otherwise you might want to keep the database files between restarts or change some JVM settings etc. do this:
```bash
$ docker run --rm --name kvd -ti -v /my/volume:/storage -p 3030:3030 -e JAVA_OPTS="-verbose:gc -XX:+UnlockExperimentalVMOptions -XX:+UseZGC" agebe/kvd:0.0.2 --storage /storage --log-level debug
```

### Running the server from source

* Clone kvd from this repository
* Install [gradle](https://gradle.org/)
* To start the server execute
```bash
$ gradle run
```
In this case the database is written to $HOME/.kvd

## Client examples

For the example below to work you need to add the kvd-client library as a dependency in your project.

TODO add gradle example

The examples below show how to use the client API to access the database

### Write a value to the database
```java
    try(KvdClient client = new KvdClient("localhost:3030")) {
      try(DataOutputStream out = new DataOutputStream(client.put("simplestream"))) {
        out.writeLong(42);
      }
    }
```

### Read a value from the database
```java
    try(KvdClient client = new KvdClient("localhost:3030")) {
      InputStream i = client.get("simplestream");
      if(i != null) {
        try(DataInputStream in = new DataInputStream(i)) {
         assertEquals(42, in.readLong());
        }
      } else {
        throw new RuntimeException("value missing");
      }
    }
```

## Future work
* let the server cleanup (delete) values that come with expiry information
* configurable max size for single values
* configurable max size for database. This might automatically drop values to make room for new values (LRU)
* client/server network transport encryption
* server authentication
