package kvd.client;

import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

public class KvdPut extends KvdRunnable {

  private static final Logger log = LoggerFactory.getLogger(KvdPut.class);

  private String key;

  private InputStream in;

  public KvdPut(AtomicBoolean run, ClientBackend backend, String key, InputStream in) {
    super(run, backend);
    this.key = key;
    this.in = in;
  }

  @Override
  public void run() {
    int channelId = 0;
    try {
      channelId = backend.createChannel();
      backend.sendAsync(new Packet(PacketType.PUT_INIT, channelId, Utils.toUTF8(key)));
      while(isRun()) {
        byte[] buf = new byte[16*1024];
        int read = in.read(buf);
        if(read < 0) {
          break;
        } else if(read > 0) {
          if(read == buf.length) {
            backend.sendAsync(new Packet(PacketType.PUT_DATA, channelId, buf));
          } else {
            byte[] send = new byte[read];
            System.arraycopy(buf, 0, send, 0, read);
            backend.sendAsync(new Packet(PacketType.PUT_DATA, channelId, send));
          }
        }
      }
      backend.sendAsync(new Packet(PacketType.PUT_FINISH, channelId));
      BlockingQueue<Packet> queue = backend.getReceiveChannel(channelId);
      while(isRun()) {
        Packet packet = queue.poll(1, TimeUnit.SECONDS);
        if(packet != null) {
          if(PacketType.PUT_COMPLETE.equals(packet.getType())) {
            log.trace("put complete");
            break;
          } else {
            throw new KvdException("received unexpected packet " + packet.getType());
          }
        }
      }
    } catch(Exception e) {
      log.warn("put failure", e);
    } finally {
      backend.closeChannel(channelId);
      Utils.closeQuietly(in);
    }
  }

}
