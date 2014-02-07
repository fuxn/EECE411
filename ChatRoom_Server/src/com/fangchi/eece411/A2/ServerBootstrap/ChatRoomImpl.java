package com.fangchi.eece411.A2.ServerBootstrap;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import com.fangchi.eece411.A2.Interface.ChatRoomInterface;
import com.fangchi.eece411.A2.Interface.ChatUserInterface;
import com.fangchi.eece411.A2.Utilities.Message;
import com.fangchi.eece411.A2.Utilities.MessageQueue;

public class ChatRoomImpl extends UnicastRemoteObject implements
		ChatRoomInterface {

	/**
	 * 
	 */
	private static final long serialVersionUID = -69354807108325143L;
	private List<Object> clients;
	private static MessageQueue queue;

	protected ChatRoomImpl() throws RemoteException {
		super();
		this.clients = new ArrayList<Object>();
		queue = new MessageQueue();
	}

	// wait until a message received from client, broadcast the message to other
	// clients
	public void waitForMessages() {
		while (true) {
			Message message = null;
			try {
				message = queue.dequeue();
				System.out.println("Message Received: " + message);
			} catch (InterruptedException ie) {
				System.out
						.println("Server has encountered an Interrupted Exception, please restart ChatRoom_Server");
				ie.printStackTrace();
				break;
			}
			try {
				this.broadcastMessage(message);
			} catch (RemoteException re) {
				System.out.println("Server has encountered a Remote Exception");
				re.printStackTrace();
			}
		}
	}

	//broadcast messages to other clients
	public boolean broadcastMessage(Message message) throws RemoteException {
		for (Object client : this.clients) {
			if (!(client instanceof ChatUserInterface)) {
				System.out.println("Wrong type of client");
				return false;
			}

			if (client.equals(message.getClient()))
				continue;

			if (((ChatUserInterface) client).broadCastMessage(message
					.getClient().getUserName() + ":>" + message.getMessage())) {
				System.out.println("broadcast message from "
						+ message.getClient().getUserName() + " succeed");
			} else {
				System.out.println("broadcast message from "
						+ message.getClient().getUserName() + " failed");
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean register(ChatUserInterface client) throws RemoteException {
		if (client == null) {
			return false;
		}

		this.clients.add(client);
		return true;
	}

	@Override
	public boolean postMessage(Message message) throws RemoteException {
		if (message == null)
			return false;

		System.out.println(message.getMessage());
		queue.enqueue(message);
		return true;
	}

	@Override
	public boolean unregister(ChatUserInterface client) throws RemoteException {
		if (client == null || !this.clients.contains(client)) {
			System.out.println("Client does not exist");
			return false;
		}

		return this.clients.remove(client);
	}

}
