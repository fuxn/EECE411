package NIO.Client;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import NIO.Dispatcher;
import NIO.EventHandler;
import Utilities.Message.MessageUtilities;
import Utilities.Message.RemoteMessage;

public class ReadReplyEventHandler implements EventHandler {
	private int errorCode;
	private byte[] value;
	private ByteBuffer errorCodeBuffer = ByteBuffer.allocate(1);
	private ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

	@Override
	public synchronized void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		RemoteMessage message = (RemoteMessage) handle.attachment();
		int c = message.getMessage().array()[0];

		socketChannel.read(errorCodeBuffer);
		errorCodeBuffer.flip();
		this.errorCode = errorCodeBuffer.array()[0];

		if (MessageUtilities.isGetCommand(c)) {
			socketChannel.read(valueBuffer);
			valueBuffer.flip();
			this.value = valueBuffer.array();
		}
		valueBuffer.clear();
		errorCodeBuffer.clear();

		SelectionKey serverHandle = message.getServerHandle();
		if (serverHandle != null) {
			Dispatcher.response(serverHandle, MessageUtilities
					.formateReplyMessage(this.errorCode, this.value));
		}

		socketChannel.close();
	}

}
