package kvd.client;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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

public class KvdGet extends KvdRunnable {

  private static final Logger log = LoggerFactory.getLogger(KvdGet.class);

  private String key;

  private CompletableFuture<InputStream> future;

  public KvdGet(AtomicBoolean run, ClientBackend backend, String key, CompletableFuture<InputStream> future) {
    super(run, backend);
    this.key = key;
    this.future = future;
  }

  @Override
  public void run() {
    int channelId = 0;
    try(PipedOutputStream src = new PipedOutputStream()) {
      channelId = backend.createChannel();
      backend.sendAsync(new Packet(PacketType.GET_INIT, channelId, Utils.toUTF8(key)));
      BlockingQueue<Packet> queue = backend.getReceiveChannel(channelId);
      while(isRun()) {
        Packet packet = queue.poll(1, TimeUnit.SECONDS);
        if(packet != null) {
          if(PacketType.GET_DATA.equals(packet.getType())) {
            if(!future.isDone()) {
              future.complete(new PipedInputStream(src, 64 * 1024));
            }
            src.write(packet.getBody());
          } else if(PacketType.GET_FINISH.equals(packet.getType())) {
            break;
          } else {
            throw new KvdException("received unexpected packet " + packet.getType());
          }
        }
      }
    } catch(Exception e) {
      log.warn("get failure", e);
    } finally {
      if(!future.isDone()) {
        future.complete(null);
      }
      backend.closeChannel(channelId);
    }
  }

}
