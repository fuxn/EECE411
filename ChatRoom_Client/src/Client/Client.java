package Client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;

import Interface.ChatRoomInterface;
import Utilities.Message;
import Utilities.MessageQueue;

public class Client {
	
	private String userName;
	private ChatRoomInterface chatRoom;
	private ChatUserImpl client;

	public Client(String userName) throws RemoteException {
		super();
		this.userName = userName;
	}

	public boolean initializeClient(String host) {

		try {
			this.client = new ChatUserImpl(this.userName);
			if (!this
					.connectToServer(host))
				return this.autoRetry();
			else
				return true;

		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

		return false;
	}

	public boolean autoRetry() throws MalformedURLException, RemoteException,
			NotBoundException {
		int count = 5;
		while (count > 0) {
			if (this.connectToServer("dhcp-128-189-249-196.ubcsecure.wireless.ubc.ca"))
				return true;
		}
		System.out.println("Connect to Server failed..");
		return false;
	}

	public boolean unregister() {
		if (this.chatRoom == null) {
			return false;
		}

		try {
			return this.chatRoom.unregister(this.client);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return false;
	}

	public boolean post(String message) {
		if (message == null || this.chatRoom == null) {
			return false;
		}

		try {
			System.out.println("posting");
			return this.chatRoom.postMessage(new Message(this.client,message));
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return false;
	}

	private Boolean connectToServer(String host) throws MalformedURLException,
			RemoteException, NotBoundException {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}

		this.chatRoom = (ChatRoomInterface) Naming.lookup("rmi://" + host
				+ "/ChatRoom");

		return this.register();
	}

	private boolean register() {
		if (this.chatRoom == null) {
			return false;
		}
		try {
			if (!this.chatRoom.register(this.client)) {
				System.out.println("Failed to register to chat room..");
				return false;
			} else {
				System.out.println("Successed to register to chat room..");
				return true;
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return false;
	}

	public MessageQueue getMessageQueue() {
		return this.client.queue;
	}

}
