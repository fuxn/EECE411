package NIO_Client;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;
import Utilities.Message.MessageUtilities;

public class ReadReplyEventHandler implements EventHandler {
	private int errorCode;
	private String value;

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		System.out.println("ReadReplyEventHandler");
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		ByteBuffer errorCodeBuffer = ByteBuffer.allocate(1);
		ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

		int errorCodelength = socketChannel.read(errorCodeBuffer);
		System.out.println("error code length " + errorCodelength);
		errorCodeBuffer.flip();

		byte[] error = new byte[errorCodeBuffer.limit()];
		errorCodeBuffer.get(error);

		this.errorCode = error[0];

		int length = socketChannel.read(valueBuffer);
		if (length == -1) {
			return;
		}
		this.value = MessageUtilities.checkReplyValue(socketChannel,
				valueBuffer);
		System.out.println(this.value);

		errorCodeBuffer.clear();
		valueBuffer.clear();

		socketChannel.close();
		handle.cancel();
	}

	public byte[] getReplyMessage() {
		return MessageUtilities.formateReplyMessage(
				Integer.valueOf(this.errorCode), this.value);
	}
}
