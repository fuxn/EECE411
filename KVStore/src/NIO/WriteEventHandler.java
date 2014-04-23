package NIO;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WriteEventHandler implements EventHandler {

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		ByteBuffer buffer = (ByteBuffer) handle.attachment();

		int totalByte = 0;
		while (buffer.hasRemaining()) {
			totalByte +=socketChannel.write(buffer);
		}
		System.out.println("total write byte "+ totalByte);
		buffer.flip();
		socketChannel.close();// Close connection
	}
}