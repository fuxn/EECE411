package Server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import Interface.ChatRoomInterface;
import Interface.ChatUserInterface;

public class ChatRoomImpl extends UnicastRemoteObject implements
		ChatRoomInterface {

	private List<Object> clients;

	protected ChatRoomImpl() throws RemoteException {
		super();
		this.clients = new ArrayList<Object>();
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

		return this.broadcastMessage(message);
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
