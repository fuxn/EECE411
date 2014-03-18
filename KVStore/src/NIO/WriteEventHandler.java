package NIO;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WriteEventHandler implements EventHandler {

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		System.out.println("handel write ");
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		ByteBuffer inputBuffer = (ByteBuffer) handle.attachment();
		while (inputBuffer.hasRemaining()) {
			socketChannel.write(inputBuffer);
		}
		socketChannel.close(); // Close connection
	}

}