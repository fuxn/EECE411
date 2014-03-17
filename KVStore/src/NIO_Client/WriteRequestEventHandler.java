package NIO_Client;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;

public class WriteRequestEventHandler implements EventHandler {
	private Selector selector;
	private ByteBuffer requestBuffer;

	public WriteRequestEventHandler(Selector demultiplexer,
			ByteBuffer requestBuffer) {
		this.selector = demultiplexer;
		this.requestBuffer = requestBuffer;
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		System.out.println("handel write Request");
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		socketChannel.write(this.requestBuffer);

		socketChannel.register(this.selector, SelectionKey.OP_READ);
	}
}
