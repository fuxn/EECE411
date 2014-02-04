package Server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import Interface.ChatRoomInterface;
import Interface.ChatUserInterface;
import Utilities.MessageQueue;

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

	public void waitForMessages() {
		while (true) {
			String message = null;
			try {
				message = queue.dequeue();
			} catch (InterruptedException ie) {
				System.out
						.println("Server has encountered an Interrupted Exception, please restart ChatRoom_Server");
				break;
			}
			try {
				this.broadcastMessage(message);
			} catch (RemoteException re) {
				System.out
						.println("Server has encountered a Remote Exception, please restart ChatRoom_Server");
				break;
			}
		}
	}

	public boolean broadcastMessage(String message) throws RemoteException {
		for (Object client : this.clients) {
			if (!(client instanceof ChatUserInterface)) {
				System.out.println("Wrong type of client");
				return false;
			}

			if (((ChatUserInterface) client).broadCastMessage(message)) {
				System.out.println("broadcast succeed");
			} else {
				System.out.println("boradcast failed");
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
	public boolean postMessage(String message) throws RemoteException {
		if (message == null)
			return false;

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
