package Client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import Interface.ChatRoomInterface;
import Interface.ChatUserInterface;

public class ChatUserImpl extends UnicastRemoteObject implements
		ChatUserInterface {

	private String userName;
	private ChatRoomInterface chatRoom;

	public ChatUserImpl(String userName) throws RemoteException {
		super();
		this.userName = userName;
	}

	@Override
	public boolean broadCastMessage(String message) {
		if (message == null)
			return false;

		else {
			System.out.println("Message from Server : " + message);
			return true;
		}
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
			if (!chatRoom.register(this)) {
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

	public boolean unregister() {
		if (this.chatRoom == null) {
			return false;
		}

		try {
			return chatRoom.unregister(this);
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
			return this.chatRoom.postMessage(this.userName + ": " + message);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean initializeClient() {
		try {

			if (!this
					.connectToServer("dhcp-128-189-249-196.ubcsecure.wireless.ubc.ca"))
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

}
