package NIO_Client;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import NIO.Dispatcher;
import NIO.EventHandler;
import Utilities.CommandEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Message.RemoteMessage;
import Utilities.Message.Requests;

public class ReadReplyEventHandler implements EventHandler {
	private int errorCode;
	private byte[] value;

	private ByteBuffer errorCodeBuffer = ByteBuffer.allocate(1);
	private ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

	public ReadReplyEventHandler() {

	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {

		SocketChannel socketChannel = (SocketChannel) handle.channel();

		int byteReceived = 0;
		while (byteReceived != 1) {
			byteReceived = socketChannel.read(errorCodeBuffer);
		}
		System.out.println("error code length " + byteReceived);
		errorCodeBuffer.flip();

		byte[] error = new byte[errorCodeBuffer.limit()];
		errorCodeBuffer.get(error);

		this.errorCode = error[0];

		RemoteMessage message = (RemoteMessage) handle.attachment();

		int c = message.getMessage().array()[0];

		MessageUtilities.checkReplyValue(socketChannel, c, this.valueBuffer);

		this.valueBuffer.flip();
		this.value = new byte[this.valueBuffer.limit()];
		this.valueBuffer.get(value);

		errorCodeBuffer.clear();
		valueBuffer.clear();

		SelectionKey serverHandle = message.getServerHandle();
		serverHandle.interestOps(SelectionKey.OP_WRITE);
		serverHandle.attach(new Requests(CommandEnum.PUT, ByteBuffer
				.wrap(MessageUtilities.formateReplyMessage(
						Integer.valueOf(this.errorCode), this.value))));

		Dispatcher.getDemultiplexer().wakeup();

		socketChannel.close();
	}

	public byte[] getReplyMessage() {
		return MessageUtilities.formateReplyMessage(
				Integer.valueOf(this.errorCode), this.value);
	}
}
