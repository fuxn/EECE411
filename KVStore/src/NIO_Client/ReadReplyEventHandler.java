package NIO_Client;

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

	public ReadReplyEventHandler() {

	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		SocketChannel socketChannel = (SocketChannel) handle.channel();
		RemoteMessage message = (RemoteMessage) handle.attachment();
		int c = message.getMessage().array()[0];

		ByteBuffer buffer = ByteBuffer.allocate(1);
		socketChannel.read(buffer);
		buffer.flip();
		this.errorCode = buffer.array()[0];

		if (MessageUtilities.isCheckReplyValue(c)) {
			buffer = ByteBuffer.allocate(1024);
			socketChannel.read(buffer);
			buffer.flip();
			this.value = buffer.array();
		}
		buffer.clear();

		SelectionKey serverHandle = message.getServerHandle();
		serverHandle.interestOps(SelectionKey.OP_WRITE);
		serverHandle.attach(ByteBuffer.wrap(MessageUtilities
				.formateReplyMessage(this.errorCode, this.value)));

		Dispatcher.getDemultiplexer().wakeup();

		socketChannel.close();
	}

	public byte[] getReplyMessage() {
		return MessageUtilities.formateReplyMessage(
				Integer.valueOf(this.errorCode), this.value);
	}
}
