package com.fangchi.eece411.A2.Utilities;

import java.io.Serializable;
import java.rmi.RemoteException;

import com.fangchi.eece411.A2.Interface.ChatUserInterface;

public class Message implements Serializable {

	private static final long serialVersionUID = -69354807108325143L;
	ChatUserInterface client;
	String message;

	public Message(ChatUserInterface client, String message)
			throws RemoteException {
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
