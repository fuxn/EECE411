package Utilities.Message;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

public class RemoteMessage {
	private Selector serverSelector;
	private SelectionKey serverHandle;
	private ByteBuffer message;

	public RemoteMessage(Selector serverSelector,
			SelectionKey serverHandle, ByteBuffer message) {

		this.message = message;
		this.serverHandle = serverHandle;
		this.serverSelector = serverSelector;
	}

	public Selector getServerSelector() {
		return this.serverSelector;
	}

	public SelectionKey getServerHandle() {
		return this.serverHandle;
	}

	public ByteBuffer getMessage() {
		return this.message;
	}
}