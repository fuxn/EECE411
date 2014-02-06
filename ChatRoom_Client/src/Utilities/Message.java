package Utilities;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import Interface.ChatUserInterface;
import Interface.MessageInterface;

public class Message extends UnicastRemoteObject implements MessageInterface{

	ChatUserInterface client;
	String message;

	public Message(ChatUserInterface client, String message) throws RemoteException{
		this.client = client;
		this.message = message;
	}

	public ChatUserInterface getClient() {
		return client;
	}

	public String getMessage() {
		return message;
	}

}
