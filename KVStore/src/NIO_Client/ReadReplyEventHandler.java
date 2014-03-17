package NIO_Client;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import NIO.EventHandler;
import Utilities.Message.MessageUtilities;

public class ReadReplyEventHandler implements EventHandler {
	private int errorCode;
	private String value;

	private int command;

	public ReadReplyEventHandler(int command) {
		this.command = command;
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		ByteBuffer errorCodeBuffer = ByteBuffer.allocate(1);
		ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

		int errorCodelength = socketChannel.read(errorCodeBuffer);
		System.out.println("error code length " + errorCodelength);
		errorCodeBuffer.flip();

		byte[] error = new byte[errorCodeBuffer.limit()];
		errorCodeBuffer.get(error);

		this.errorCode = error[0];

		this.value = MessageUtilities.checkReplyValue(socketChannel,
				this.command, valueBuffer);

		errorCodeBuffer.clear();
		valueBuffer.clear();

		socketChannel.close();
	}

	public byte[] getReplyMessage() {
		return MessageUtilities.formateReplyMessage(
				Integer.valueOf(this.errorCode), this.value);
	}
}
