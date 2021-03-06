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
import kvd.client.KvdClient;
import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.Version;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.concurrent.LockMode;
import kvd.server.storage.concurrent.OptimisticLockStorageBackend;
import kvd.server.storage.concurrent.PessimisticLockStorageBackend;
import kvd.server.storage.fs.FileStorageBackend;
import kvd.server.storage.mem.MemStorageBackend;

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

    @Parameter(names= {"--concurrency-control", "-cc"}, description="concurrency control: none,"
        + " optimistic (OPTW or OPTRW), pessimistic (PESW or PESRW)")
    public ConcurrencyControl concurrency = ConcurrencyControl.NONE;

  }

  private SimpleSocketServer socketServer;

  private SocketConnectHandler handler;

  private StorageBackend setupConcurrencyControl(KvdOptions options, StorageBackend downstream) {
    if(options.concurrency == null || ConcurrencyControl.NONE.equals(options.concurrency)) {
      return downstream;
    } else if (ConcurrencyControl.OPTRW.equals(options.concurrency)) {
      return new OptimisticLockStorageBackend(downstream, LockMode.READWRITE);
    } else if(ConcurrencyControl.OPTW.equals(options.concurrency)) {
      return new OptimisticLockStorageBackend(downstream, LockMode.WRITEONLY);
    } else if (ConcurrencyControl.PESRW.equals(options.concurrency)) {
      return new PessimisticLockStorageBackend(downstream, LockMode.READWRITE);
    } else if(ConcurrencyControl.PESW.equals(options.concurrency)) {
      return new PessimisticLockStorageBackend(downstream, LockMode.WRITEONLY);
    } else {
      throw new KvdException(String.format("concurrency control '%s' not implemented", options.concurrency));
    }
  }

  private StorageBackend createStorageBackend(KvdOptions options) {
    if(StringUtils.startsWith(options.storage, "file:")) {
      String directory = StringUtils.removeStart(options.storage, "file:");
      log.info("using file storage backend at directory '{}'", directory);
      return new FileStorageBackend(new File(directory));
    } else if(StringUtils.startsWith(options.storage, "mem:")) {
      log.info("using mem storage backend");
      return new MemStorageBackend();
    } else {
      throw new KvdException("unknown storage backend: "+ options.storage);
    }
  }

  private void logJvmInfo() {
    long maxMem = Runtime.getRuntime().maxMemory();
    String maxMemString = maxMem == Long.MAX_VALUE?"unlimited":Utils.humanReadableByteCountBin(maxMem);
    log.info("jvm '{} {} {}', processors '{}', max jvm memory '{}'",
        System.getProperty("java.vm.name"),
        System.getProperty("java.vm.version"),
        System.getProperty("java.vm.vendor"),
        Runtime.getRuntime().availableProcessors(),
        maxMemString);
  }

  public void run(KvdOptions options) {
    setLogLevel("kvd", options.logLevel);
    Version version = getVersion();
    if(version != null) {
      log.info("{}", version.version());
    }
    logJvmInfo();
    handler = new SocketConnectHandler(options.maxClients,
        setupConcurrencyControl(options, createStorageBackend(options)));
    socketServer = new SimpleSocketServer(options.port, handler);
    socketServer.start();
    log.info("started socket server on port '{}', max clients '{}'", socketServer.getLocalPort(), options.maxClients);
  }

  private SimpleSocketServer getSocketServer() {
    return socketServer;
  }

  public void shutdown() {
    getSocketServer().stop();
  }

  public KvdClient newLocalClient() {
    return new KvdClient("localhost:"+getLocalPort());
  }

  public int getLocalPort() {
    return getSocketServer().getLocalPort();
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
