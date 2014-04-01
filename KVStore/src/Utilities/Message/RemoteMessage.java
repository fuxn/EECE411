package Utilities.Message;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class RemoteMessage {
	private SelectionKey serverHandle;
	private ByteBuffer message;
	private Integer key;
	private String coordinator;

	public RemoteMessage(SelectionKey serverHandle, Integer key, ByteBuffer message,String coordinator) {

		this.key = key;
		this.message = message;
		this.serverHandle = serverHandle;
		this.setCoordinator(coordinator);
	}

	public SelectionKey getServerHandle() {
		return this.serverHandle;
	}

	public ByteBuffer getMessage() {
		return this.message;
	}
	
	public Integer getKey(){
		return this.key;
	}

	public String getCoordinator() {
		return coordinator;
	}

	public void setCoordinator(String coordinator) {
		this.coordinator = coordinator;
	}
}