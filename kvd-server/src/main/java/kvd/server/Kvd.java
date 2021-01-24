/*
 * Copyright 2020 Andre Gebers
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kvd.server;

import java.io.File;
import java.net.URL;
import java.util.jar.Manifest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.Version;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.fs.FileStorage;
import kvd.server.storage.mem.MemStorage;

public class Kvd {

  private static final Logger log = LoggerFactory.getLogger(Kvd.class);

  public static class KvdOptions {

    @Parameter(names="--help", help=true, description="show usage")
    public boolean help;

    @Parameter(names="--port", description="port to listen on")
    public int port = 3030;

    @Parameter(names="--storage", description="kvd storage backend, file:<directory> or mem:")
    public String storage = "file:" + new File(Utils.getUserHome().getAbsolutePath(), ".kvd").getAbsolutePath();

    @Parameter(names="--max-clients", description="maximum number of clients that can connect to the server at the same time")
    public int maxClients = 100;

    @Parameter(names="--log-level", description="logback log level (trace, debug, info, warn, error, all, off)")
    public String logLevel = "info";

  }

  private SimpleSocketServer socketServer;

  private SocketConnectHandler handler;

  private StorageBackend createStorageBackend(KvdOptions options) {
    if(StringUtils.startsWith(options.storage, "file:")) {
      String directory = StringUtils.removeStart(options.storage, "file:");
      log.info("using file storage backend at directory '{}'", directory);
      return new FileStorage(new File(directory));
    } else if(StringUtils.startsWith(options.storage, "mem:")) {
      log.info("using mem storage backend");
      return new MemStorage();
    } else {
      throw new KvdException("unknown storage backend: "+ options.storage);
    }
  }

  public void run(KvdOptions options) {
    setLogLevel("kvd", options.logLevel);
    Version version = getVersion();
    if(version != null) {
      log.info("{}", version.version());
    }
    handler = new SocketConnectHandler(options.maxClients, createStorageBackend(options));
    socketServer = new SimpleSocketServer(options.port, handler);
    socketServer.start();
    log.info("started socket server on port '{}', max clients '{}'", socketServer.getLocalPort(), options.maxClients);
  }

  public SimpleSocketServer getSocketServer() {
    return socketServer;
  }

  private Version getVersion() {
    try {
      URL manifestUrl = new URL(StringUtils.substringBeforeLast(
          Kvd.class.getResource("Kvd.class").toString(), "!")+"!/META-INF/MANIFEST.MF");
      return new Version(new Manifest(manifestUrl.openStream()));
    } catch(Exception e) {
      log.trace("failed to determine version", e);
    }
    return null;
  }

  public static void main(String[] args) {
    KvdOptions options = new KvdOptions();
    JCommander jcommander = JCommander.newBuilder().addObject(options).build();
    jcommander.setProgramName("kvd");
    jcommander.parse(args);
    if(options.help) {
      jcommander.usage();
      return;
    } else {
      new Kvd().run(options);
    }
  }

  public static void setLogLevel(String name, String level) {
    LoggerContext loggerContext = (LoggerContext)LoggerFactory.getILoggerFactory();
    Logger logger = loggerContext.getLogger(name);
    Level l = (StringUtils.isBlank(level) || StringUtils.equalsIgnoreCase("PARENT", level))?
        null:Level.valueOf(StringUtils.upperCase(level));
    ((ch.qos.logback.classic.Logger)logger).setLevel(l);
  }

}
