package NIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class WriteEventHandler implements EventHandler{

	@Override
	public void handleEvent(SelectionKey handle) throws Exception{
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		ByteBuffer buffer = (ByteBuffer) handle.attachment();

		int attempts = 0;
		int bytesProduced = 0; 
		SelectionKey key = null;
		Selector writeSelector = null;
		while (buffer.hasRemaining()) {
			try {
				int len = socketChannel.write(buffer);
				attempts++;

				if (len < 0)
					throw new Exception();

				bytesProduced += len;  
				
				if (len == 0) {
					if (writeSelector == null) {

						writeSelector = SelectorFactory.getSelector();

						if (writeSelector == null) {
							continue;

						}

					}

					key = socketChannel.register(writeSelector,
							SelectionKey.OP_WRITE);
					if (writeSelector.select(1000) == 0) {
						if (attempts > 2)
							throw new IOException("Client disconnected");

					} else {

						attempts--;

					}

				} else {

					attempts = 0;

				}

			} finally {
				if (key != null) {
					key.cancel();

					key = null;
				}

			}

			if (writeSelector != null) {

				writeSelector.selectNow();

				SelectorFactory.returnSelector(writeSelector);
			}
		}
		buffer.flip();
		socketChannel.close();// Close connection
	}
}