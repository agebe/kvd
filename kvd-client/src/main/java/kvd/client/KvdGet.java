package kvd.client;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

public class KvdGet implements Abortable {

  private static final Logger log = LoggerFactory.getLogger(KvdGet.class);

  private ClientBackend backend;

  private String key;

  private CompletableFuture<InputStream> future = new CompletableFuture<>();

  private int channelId;

  private Consumer<Abortable> closeListener;

  private KvdGetInputStream stream;

  private AtomicBoolean closed = new AtomicBoolean();

  public KvdGet(ClientBackend backend, String key, Consumer<Abortable> closeListener) {
    this.backend = backend;
    this.key = key;
    this.closeListener = closeListener;
    stream = new KvdGetInputStream(this::closeInternal);
  }

  public void start() {
    channelId = backend.createChannel(this::receive);
    try {
      backend.sendAsync(new Packet(PacketType.GET_INIT, channelId, Utils.toUTF8(key)));
    } catch(Exception e) {
      try {
        close();
      } catch(Exception e2) {
        // ignore
      }
      throw new KvdException("get failed", e);
    }
  }

  @Override
  public void abort() {
    future.completeExceptionally(new KvdException("aborted"));
    stream.abort();
    close();
  }

  private void close() {
    closeInternal();
    stream.close();
  }

  private void closeInternal() {
    if(!closed.getAndSet(true)) {
      this.closeListener.accept(this);
      future.complete(null);
      backend.closeChannel(channelId);
    }
  }

  public void receive(Packet packet) {
    if(PacketType.GET_DATA.equals(packet.getType())) {
      future.complete(stream);
      stream.fill(packet.getBody());
    } else if(PacketType.GET_FINISH.equals(packet.getType())) {
      close();
    } else if(PacketType.GET_ABORT.equals(packet.getType())) {
      abort();
    } else {
      log.error("received unexpected packet " + packet.getType());
      future.completeExceptionally(new KvdException("received unexpected packet " + packet.getType()));
      abort();
    }
  }

  public CompletableFuture<InputStream> getFuture() {
    return future;
  }

  @Override
  public String toString() {
    return "GET " + key;
  }

}
