package NIO;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class AcceptEventHandler implements EventHandler {
	private Selector selector;

	public AcceptEventHandler(Selector demultiplexer) {
		this.selector = demultiplexer;
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) handle
				.channel();
		SocketChannel socketChannel = serverSocketChannel.accept();
		if (socketChannel != null) {
			socketChannel.configureBlocking(false);
			socketChannel.register(selector, SelectionKey.OP_READ);   
		}
	}

}
