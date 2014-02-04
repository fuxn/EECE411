package Client;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import Interface.ChatRoomInterface;
import Interface.ChatUserInterface;

public class ChatUserImpl extends UnicastRemoteObject implements
		ChatUserInterface {

	private String userName;

	protected ChatUserImpl(String userName) throws RemoteException {
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

	public boolean register(ChatRoomInterface chatRoom) {
		if (chatRoom == null) {
			return false;
		}
		try {
			if (!chatRoom.register(this)) {
				System.out.println("Failed to connected to chat room..");
				return false;
			} else {
				System.out.println("Successed to connected to chat room..");
				return true;
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return false;
	}

	public boolean unregister(ChatRoomInterface chatRoom) {
		if (chatRoom == null) {
			return false;
		}

		try {
			return chatRoom.unregister(this);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return false;
	}

	public void send(ChatRoomInterface chatRoom, String message) {
		if (message == null || chatRoom == null) {
			return;
		}

		try {
			chatRoom.postMessage(this.userName + ": " + message);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

}
