package kvd.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

public class KvdRemove extends KvdRunnable {

  private static final Logger log = LoggerFactory.getLogger(KvdRemove.class);

  private String key;

  private CompletableFuture<Boolean> future;

  public KvdRemove(AtomicBoolean run, ClientBackend backend, String key, CompletableFuture<Boolean> future) {
    super(run, backend);
    this.key = key;
    this.future = future;
  }

  @Override
  public void run() {
    int channelId = 0;
    try {
      channelId = backend.createChannel();
      backend.sendAsync(new Packet(PacketType.REMOVE_REQUEST, channelId, Utils.toUTF8(key)));
      BlockingQueue<Packet> queue = backend.getReceiveChannel(channelId);
      while(isRun()) {
        Packet packet = queue.poll(1, TimeUnit.SECONDS);
        if(packet != null) {
          if(PacketType.REMOVE_RESPONSE.equals(packet.getType())) {
            byte[] buf = packet.getBody();
            if((buf != null) && (buf.length >= 1)) {
              future.complete((buf[0] == 1));
            } else {
              throw new KvdException("invalid response");
            }
            break;
          } else {
            throw new KvdException("received unexpected packet " + packet.getType());
          }
        }
      }
    } catch(Exception e) {
      log.warn("remove failure", e);
      future.completeExceptionally(e);
    } finally {
      backend.closeChannel(channelId);
    }
  }

}
