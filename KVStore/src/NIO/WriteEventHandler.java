package NIO;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import Utilities.CommandEnum;
import Utilities.Message.Requests;

public class WriteEventHandler implements EventHandler {

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		Requests requests = (Requests) handle.attachment();
		ByteBuffer buffer = requests.getReply();
		while (buffer.hasRemaining()) {
			socketChannel.write(buffer);
		}
		socketChannel.close(); // Close connection

		if (CommandEnum.ANNOUNCE_FAILURE.equals(requests.getCommand()));
		//	System.exit(0);
	}

}