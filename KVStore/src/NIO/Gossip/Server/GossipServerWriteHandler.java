package NIO.Gossip.Server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;
import NIO.SelectorFactory;

public class GossipServerWriteHandler implements EventHandler {

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		ByteBuffer buffer = (ByteBuffer) handle.attachment();

		int totalByte = 0;
		while (buffer.hasRemaining()) {
			totalByte+=socketChannel.write(buffer);
		}
		System.out.println("replica server write byte "+ totalByte);
		buffer.flip();
		socketChannel.close();// Close connection

	}

}