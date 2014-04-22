package NIO.Client.Replica.Server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;
import NIO.SelectorFactory;

public class ReplicaServerWriteHandler implements EventHandler {

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		ByteBuffer buffer = (ByteBuffer) handle.attachment();

		while (buffer.hasRemaining()) {
			socketChannel.write(buffer);
		}
		buffer.flip();
		socketChannel.close();// Close connection

	}

}