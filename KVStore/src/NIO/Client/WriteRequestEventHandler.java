package NIO.Client;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;
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
		while (m.hasRemaining()) {
			socketChannel.write(m);
		}
		m.flip();

		socketChannel.register(this.selector, SelectionKey.OP_READ,message);
	}
}
