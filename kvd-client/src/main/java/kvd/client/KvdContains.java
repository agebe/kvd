package kvd.client;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

public class KvdContains implements Abortable {

  private static final Logger log = LoggerFactory.getLogger(KvdContains.class);

  private CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();

  private ClientBackend backend;

  private String key;

  private int channelId;

  private Consumer<Abortable> closeListener;

  public KvdContains(ClientBackend backend, String key, Consumer<Abortable> closeListener) {
    this.backend = backend;
    this.key = key;
    this.closeListener = closeListener;
  }

  public void start() {
    channelId = backend.createChannel(this::receive);
    try {
      backend.sendAsync(new Packet(PacketType.CONTAINS_REQUEST, channelId, Utils.toUTF8(key)));
    } catch(Exception e) {
      try {
        close();
      } catch(Exception e2) {
        // ignore
      }
      throw new KvdException("contains failed", e);
    }
  }

  @Override
  public void abort() {
    future.completeExceptionally(new KvdException("aborted"));
    close();
  }

  private void close() {
    backend.closeChannel(channelId);
    this.closeListener.accept(this);
  }

  public void receive(Packet packet) {
    try {
      close();
    } catch(Exception e) {
      log.error("contains close failed", e);
    }
    if(PacketType.CONTAINS_RESPONSE.equals(packet.getType())) {
      byte[] buf = packet.getBody();
      if((buf != null) && (buf.length >= 1)) {
        future.complete((buf[0] == 1));
      } else {
        log.error("invalid server response");
        future.completeExceptionally(new KvdException("invalid server response"));
      }
    } else {
      log.error("received unexpected packet '{}'", packet.getType());
      future.completeExceptionally(new KvdException("received unexpected packet " + packet.getType()));
    }
  }

  public CompletableFuture<Boolean> getFuture() {
    return future;
  }

  @Override
  public String toString() {
    return "CONTAINS " + key;
  }

}
