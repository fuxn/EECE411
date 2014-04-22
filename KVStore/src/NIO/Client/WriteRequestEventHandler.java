package NIO.Client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;
import NIO.SelectorFactory;
import Utilities.Message.RemoteMessage;

public class WriteRequestEventHandler implements EventHandler {
	private Selector selector;

	public WriteRequestEventHandler(Selector demultiplexer) {
		this.selector = demultiplexer;
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		RemoteMessage message = (RemoteMessage) handle.attachment();
		ByteBuffer m = message.getMessage();

		int attempts = 0;
		int bytesProduced = 0;
		SelectionKey key = null;
		Selector writeSelector = null;
		while (m.hasRemaining()) {
			try {
				int len = socketChannel.write(m);
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
		m.flip();

		socketChannel.register(this.selector, SelectionKey.OP_READ, message);
	}
}
