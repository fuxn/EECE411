package NIO.Client.Replica;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import KVStore.ConsistentHash;
import NIO.Dispatcher;
import NIO.EventHandler;
import Utilities.CommandEnum;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Message.RemoteMessage;

public class ReadACKEventHandler implements EventHandler {
	private int errorCode;
	private byte[] value;
	private int version;

	private ByteBuffer errorBuffer = ByteBuffer.allocate(1);
	private ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {

		SocketChannel socketChannel = (SocketChannel) handle.channel();
		RemoteMessage message = (RemoteMessage) handle.attachment();
		int c = message.getMessage().array()[0];

		socketChannel.read(this.errorBuffer);
		this.errorBuffer.flip();
		this.errorCode = this.errorBuffer.array()[0];

		if (MessageUtilities.isGetCommand(c)) {
			socketChannel.read(this.valueBuffer);
			this.valueBuffer.flip();
			this.value = this.valueBuffer.array();

			socketChannel.read(this.errorBuffer);
			this.errorBuffer.flip();
			this.version = this.errorBuffer.array()[0];

			Dispatcher.response(message.getServerHandle(),
					MessageUtilities.formateReplyMessage(errorCode, value));
		}
		this.valueBuffer.clear();
		this.errorBuffer.clear();

		socketChannel.close();

	}
}
