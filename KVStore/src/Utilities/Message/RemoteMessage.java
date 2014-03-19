package Utilities.Message;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class RemoteMessage {
	private SelectionKey serverHandle;
	private ByteBuffer message;

	public RemoteMessage(SelectionKey serverHandle, ByteBuffer message) {

		this.message = message;
		this.serverHandle = serverHandle;
	}

	public SelectionKey getServerHandle() {
		return this.serverHandle;
	}

	public ByteBuffer getMessage() {
		return this.message;
	}
}