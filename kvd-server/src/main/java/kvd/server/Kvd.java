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
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

import org.apache.commons.io.FileUtils;
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
import kvd.server.storage.mapdb.MapdbStorageBackend;
import kvd.server.storage.mem.MemStorageBackend;
import kvd.server.storage.timestamp.ExpiredKeysRemover;
import kvd.server.storage.timestamp.TimestampStorageBackend;
import kvd.server.storage.trash.AsyncTrash;
import kvd.server.util.HumanReadable;

public class Kvd {

  private static final Logger log = LoggerFactory.getLogger(Kvd.class);

  public static class KvdOptions {

    @Parameter(names="--help", help=true, description="show usage")
    public boolean help;

    @Parameter(names="--port", description="port to listen on")
    public int port = 3030;

    @Parameter(names="--datadir", description="path to data directory")
    public File datadir = new File(Utils.getUserHome().getAbsolutePath(), ".kvd");

    @Parameter(names="--default-db-name", description="name of the default database")
    public String defaultDbName = "default";

    @Parameter(names="--max-clients", description="maximum number of clients that can connect to the server at the same time")
    public int maxClients = 100;

    @Parameter(names="--log-level", description="logback log level (trace, debug, info, warn, error, all, off)")
    public String logLevel = "info";

    @Parameter(names= {"--concurrency-control", "-cc"}, description="default concurrency control, options: NONE,"
        + " optimistic (non-blocking, OPTW or OPTRW), pessimistic (blocking, PESW or PESRW)")
    public ConcurrencyControl concurrency = ConcurrencyControl.NONE;

    @Parameter(names="--default-db-type", description="type of database, MAPDB, FILE, or MEM")
    public DbType defaultDbType = DbType.MAPDB;

    @Parameter(names="--socket-so-timeout", description="server socket so timeout."
        + " Unit can be specified ms, s, m, h, d, defaults to seconds.")
    public String soTimeoutMs = "1s";

    @Parameter(names="--client-timeout", description="client timeout."
        + " Unit can be specified ms, s, m, h, d, defaults to seconds.")
    public String clientTimeoutSeconds = "10s";

    @Parameter(names="--expire-after-access", description="removes entries from the database after no access "
        + "within this fixed duration. Defaults to never expire. Duration unit can be specified ms, s, m, h, d, "
        + "defaults to seconds.")
    public String expireAfterAccess;

    @Parameter(names="--expire-after-write", description="removes entries from the database after creation "
        + " fixed duration. Defaults to never expire. Duration unit can be specified ms, s, m, h, d, "
        + "defaults to seconds.")
    public String expireAfterWrite;

    @Parameter(names="--expire-check-interval", description="how often to check for expired keys."
        + " Unit can be specified ms, s, m, h, d, defaults to seconds.")
    public String expireCheckInterval;
  }

  private SimpleSocketServer socketServer;

  private SocketConnectHandler handler;

  private ExpiredKeysRemover expiredKeysRemover;

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

  private StorageBackend createDefaultDb(KvdOptions options) throws IOException {
    File dbDir = new File(options.datadir, "db");
    File defaultDb = new File(dbDir, FNameUtils.stringToFilename(options.defaultDbName));
    File trashDir = new File(options.datadir, "trash");
    FileUtils.forceMkdir(defaultDb);
    FileUtils.forceMkdir(trashDir);
    if(DbType.FILE.equals(options.defaultDbType)) {
      log.info("default db using file storage");
      return new FileStorageBackend(defaultDb, new AsyncTrash(trashDir));
    } else if(DbType.MEM.equals(options.defaultDbType)) {
      log.info("default db using mem storage");
      return new MemStorageBackend();
    } else if(DbType.MAPDB.equals(options.defaultDbType)) {
      log.info("default db using mapdb storage");
      return new MapdbStorageBackend(defaultDb);
    } else {
      throw new KvdException("invalid default db type " + options.defaultDbType);
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

  private void setupDataDir(KvdOptions options) {
    File f = options.datadir;
    if(f == null) {
      throw new KvdException("datadir is null");
    }
    if(!f.exists()) {
      f.mkdirs();
    }
    if(!f.exists()) {
      throw new KvdException("failed to create datadir");
    }
    File rwTest = new File(f, "rwtest");
    try {
      FileUtils.writeStringToFile(rwTest, "test", "UTF8");
      if(!"test".equals(FileUtils.readFileToString(rwTest, "UTF8"))) {
        throw new KvdException(String.format("failed datadir '%s' read/write test", f.getAbsolutePath()));
      }
    } catch(Exception e) {
      throw new KvdException(String.format("failed datadir '%s' read/write test", f.getAbsolutePath(), e));
    } finally {
      FileUtils.deleteQuietly(rwTest);
    }
    log.info("setup data directory at '{}'", f.getAbsolutePath());
  }

  public void run(KvdOptions options) throws IOException {
    setLogLevel("kvd", options.logLevel);
    Version version = getVersion();
    if(version != null) {
      log.info("{}", version.version());
    }
    logJvmInfo();
    setupDataDir(options);
    TimestampStorageBackend tsb = new TimestampStorageBackend(createDefaultDb(options));
    StorageBackend sb = setupConcurrencyControl(options, tsb);
    expiredKeysRemover = new ExpiredKeysRemover(
        HumanReadable.parseDurationToMillisOrNull(options.expireAfterAccess, TimeUnit.SECONDS),
        HumanReadable.parseDurationToMillisOrNull(options.expireAfterWrite, TimeUnit.SECONDS),
        HumanReadable.parseDurationToMillisOrNull(options.expireCheckInterval, TimeUnit.SECONDS),
        sb,
        tsb.getStore());
    expiredKeysRemover.start();
    handler = new SocketConnectHandler(
        options.maxClients,
        (int)HumanReadable.parseDuration(options.soTimeoutMs, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS),
        (int)HumanReadable.parseDuration(options.clientTimeoutSeconds, TimeUnit.SECONDS, TimeUnit.SECONDS),
        sb);
    socketServer = new SimpleSocketServer(options.port, handler);
    socketServer.start();
    log.info("started socket server on port '{}', max clients '{}'", socketServer.getLocalPort(), options.maxClients);
  }

  private SimpleSocketServer getSocketServer() {
    return socketServer;
  }

  public ExpiredKeysRemover getExpiredKeysRemover() {
    return expiredKeysRemover;
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

  public static void main(String[] args) throws IOException {
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
