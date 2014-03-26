package NIO_Gossip;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;

public class WriteGossipHandler implements EventHandler {
	private Selector selector;

	public WriteGossipHandler(Selector demultiplexer) {
		this.selector = demultiplexer;
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();

		ByteBuffer m = (ByteBuffer) handle.attachment();
		while (m.hasRemaining()) {
			socketChannel.write(m);
		}
		socketChannel.close();
		handle.cancel();
	}
}
