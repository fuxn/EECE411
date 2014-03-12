package Utilities;

import java.net.Socket;

public class Message {

	private Socket client;
	private int command;
	private String key;
	private String value;

	public Message(Socket client, Integer command, byte[] key, byte[] value) {
		this.client = client;
		this.command = command;
		if (key != null)
			this.key = new String(key);
		if (value != null)
			this.value = new String(value);
	}

	public Socket getClient() {
		return this.client;
	}

	public int getCommand() {
		return this.command;
	}

	public String getKey() {
		return this.key;
	}

	public String getValue() {
		return this.value;
	}
}
