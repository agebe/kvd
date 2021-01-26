package kvd.client;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

public class KvdRemove implements Abortable {

  private static final Logger log = LoggerFactory.getLogger(KvdRemove.class);

  private ClientBackend backend;

  private String key;

  private CompletableFuture<Boolean> future = new CompletableFuture<>();

  private Consumer<Abortable> closeListener;

  private int channelId;

  public KvdRemove(ClientBackend backend, String key, Consumer<Abortable> closeListener) {
    this.backend = backend;
    this.key = key;
    this.closeListener = closeListener;
  }

  public void start() {
    channelId = backend.createChannel(this::receive);
    try {
      backend.sendAsync(new Packet(PacketType.REMOVE_REQUEST, channelId, Utils.toUTF8(key)));
    } catch(Exception e) {
      try {
        close();
      } catch(Exception e2) {
        // ignore
      }
      throw new KvdException("remove failed", e);
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


  private void receive(Packet packet) {
    try {
      close();
    } catch(Exception e) {
      log.error("remove close failed", e);
    }
    if(PacketType.REMOVE_RESPONSE.equals(packet.getType())) {
      byte[] buf = packet.getBody();
      if((buf != null) && (buf.length >= 1)) {
        future.complete((buf[0] == 1));
      } else {
        log.error("invalid response");
        future.completeExceptionally(new KvdException("invalid response"));
      }
    } else {
      log.error("received unexpected packet " + packet.getType());
      future.completeExceptionally(new KvdException("received unexpected packet " + packet.getType()));
    }
  }

  public CompletableFuture<Boolean> getFuture() {
    return future;
  }

  @Override
  public String toString() {
    return "REMOVE " + key;
  }

}
