[![Build Status](https://app.travis-ci.com/agebe/kvd.svg?branch=main)](https://app.travis-ci.com/github/agebe/kvd/builds)

# kvd

kvd is a simple streaming key value database that follows a client/server model. kvd supports following operations:

* **put**, add/replace a key/value pair
* **get**, get a value
* **contains**, check if a key exists
* **remove**, remove a key/value pair

See example code below.

A key feature of kvd is that values are streamed into and out of the data store with support for large values (tested with single value up to 8GiB).

kvd client and server are written in Java.

I've written kvd to cache calculation results that take quite some time to compute. The results are sometimes large (> 1GB) and it seems key value databases struggle with large values so I've rolled my own.

**Note: I've only tested kvd on linux/x86_64. It might work on other OS/arch combinations.**

## Server setup

kvd can be started with --help that prints out all options.

The server listens by default on TCP port 3030 and writes to $HOME/.kvd

### Docker

To start a test server for playing do this:
```bash
$ docker run --rm -ti --name kvd -p 3030:3030 agebe/kvd:0.3.0
```

Otherwise you might want to keep the database files between restarts or change some JVM settings etc. do this:
```bash
$ docker run --rm --name kvd -ti -v /my/volume:/storage -p 3030:3030 -e JAVA_OPTS="-verbose:gc -XX:+UnlockExperimentalVMOptions -XX:+UseZGC" agebe/kvd:0.3.0 --storage file:/storage --log-level debug
```

### Running the server from source

* Clone kvd from this repository
* Make sure you have jdk 11+ installed.
* Install recent version of [gradle](https://gradle.org/releases/)
* Install [protoc](https://github.com/protocolbuffers/protobuf/releases/) and make sure the protoc binary is on your $PATH
* Change dir into ./kvd-server and execute:
```bash
$ gradle run
```
In this case the database is written to $HOME/.kvd

## Client examples

For the example below to work you need to add the kvd-client library as a dependency in your project. The kvd-client library depends on slf4j, google protobuf and java 1.8+.

Gradle:
```gradle
dependencies {
  implementation 'io.github.agebe:kvd-client:0.3.0'
}
```

Maven:
```xml
<dependency>
  <groupId>io.github.agebe</groupId>
  <artifactId>kvd-client</artifactId>
  <version>0.3.0</version>
</dependency>
```

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

kvd does not bring its own serialization support but put- and get-operations are based on Java IO streams so integration with your serialization system should be easy ([json example](https://github.com/agebe/kvd/blob/main/kvd-server/src/test/java/kvd/test/JsonTest.java))

[KvdClient API Javadoc](https://javadoc.io/doc/io.github.agebe/kvd-client/latest/kvd/client/KvdClient.html)

[![javadoc](https://javadoc.io/badge2/io.github.agebe/kvd-client/javadoc.svg)](https://javadoc.io/doc/io.github.agebe/kvd-client)

## Storage Backend
kvd currently brings 2 storage backends. One is file based and survives server restarts and the other storage backend is memory based, great for nice to haves caches.

### File Storage Backend
kvd has a quite simple filesystem based storage backend. Each key/value pair is stored into a separate file with the key being the filename and the value the content of the file. Since filesystems have restrictions on filenames only lowercase letters, digits and underscores are used as is. If keys contain other characters, only the hash of the key is used as a filename (and the original key is discarded). The key is also hashed if it contains more than 200 characters.

All files are stored in a single directory and this can work quite nicely depending on your filesystem support for large directories. I've tested this on an ext4 filesystem and put/get/contains/remove operations remain fast even with millions of entries. The feature that makes this work on the ext3/ext4 filesystem is dir_index (man ext4):

> dir_index - Use hashed b-trees to speed up name lookups in large directories.  This feature is supported by ext3 and ext4 file systems, and is ignored by ext2 file systems.

You can check which ext fs features are enabled with
```bash
$ sudo dumpe2fs /dev/<my-ext-block-device>  | less
```

If you are planing to use kvd with large amounts of entries please also read up on filesystem limitations. Ext uses 1 inode per file so make sure your filesystem has enough inodes to support your anticipated database entry size. 

Also consider that files are stored in blocks so they consume more space on disk than their actual size. On ext4 this is usally 4 KiB. If your values are mostly small (<=128 byte) you might want to consider creating your filesystem with the inline_data feature (man ext4):

> inline_data - Allow data to be stored in the inode and extended attribute area.

The File Storage Backend is the default in kvd. The storage directory can be passed in via command line options on server startup like so:
```bash
$ ... --storage file:/my/storage/directory
```

### Memory Storage Backend
All key/value pairs are stored in JVM memory inside a HashMap. Values are stored as binary large objects which grow as needed and above the 2 GiB limit of java byte arrays. Make sure you start the server with enough heap space to support your in memory data store size (e.g. -Xmx8g or -XX:MaxRAMPercentage=80.0), check like so:
```bash
$ java -XX:+UseContainerSupport -XX:InitialRAMPercentage=40.0 -XX:MinRAMPercentage=20.0 -XX:MaxRAMPercentage=80.0 -XX:+PrintFlagsFinal -XshowSettings:vm -version
```

To enable the memory storage backend start with this option:
```bash
$ ... --storage mem:
```

## Future work
* write a storage backend that is more efficient, especially with small key/values
* configurable max size for single values
* configurable max size for database. This might automatically drop values to make room for new values (LRU)
* client/server network transport encryption
* server authentication
