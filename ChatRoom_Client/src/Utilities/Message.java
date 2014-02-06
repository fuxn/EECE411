package Utilities;

import Interface.ChatUserInterface;
import Interface.MessageInterface;

public class Message implements MessageInterface{

	private ChatUserInterface client;
	private String message;

	public Message(ChatUserInterface client, String message) {
		this.client = client;
		this.message = message;
	}

	public ChatUserInterface getClient() {
		return this.client;
	}

	public String getMessage() {
		return this.message;
	}

}
