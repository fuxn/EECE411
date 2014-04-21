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
	private ByteBuffer versionBuffer = ByteBuffer.allocate(1);

	private Map<byte[], Integer> valueToVersion = new HashMap<byte[], Integer>();

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

			socketChannel.read(this.versionBuffer);
			this.versionBuffer.flip();
			this.version = this.versionBuffer.array()[0];

			if (this.errorCode == ErrorEnum.SUCCESS.getCode()) {
				SelectionKey serverhandle = message.getServerHandle();
				if (ReplicaDispatcher.pendingGet.containsKey(serverhandle)) {
					Dispatcher.response(serverhandle, MessageUtilities
							.formateReplyMessage(this.errorCode, this.value));
					ReplicaDispatcher.pendingGet.remove(serverhandle);
				}

				/*
				 * if (ReplicaDispatcher.pendingGet.containsKey(serverhandle))
				 * if (ReplicaDispatcher.pendingGet.get(serverhandle) == null) {
				 * ReplicaDispatcher.pendingGet.put(serverhandle, value);
				 * this.valueToVersion.put(value, version); } else { byte[] v =
				 * ReplicaDispatcher.pendingGet .get(serverhandle); Integer
				 * versionOld = this.valueToVersion.get(v); if (this.version >
				 * versionOld) Dispatcher.response(serverhandle,
				 * MessageUtilities .formateReplyMessage(this.errorCode,
				 * this.value)); else Dispatcher.response(serverhandle,
				 * MessageUtilities .formateReplyMessage(this.errorCode, v));
				 * 
				 * ReplicaDispatcher.pendingGet.remove(serverhandle);
				 * this.valueToVersion.remove(v); }
				 */

			}
		}
		this.valueBuffer.clear();
		this.errorBuffer.clear();

		socketChannel.close();

	}
}
