package Utilities;

import java.net.Socket;

public class Message {

	private Socket client;
	private int command;
	private byte[] key;
	private byte[] value;

	public Message(Socket client, Integer command, byte[] key, byte[] value) {
		this.client = client;
		this.command = command;
		this.key = key;
		this.value = value;
	}

	public Socket getClient() {
		return this.client;
	}

	public int getCommand() {
		return this.command;
	}

	public byte[] getKey() {
		return this.key;
	}

	public byte[] getValue() {
		return this.value;
	}
}
