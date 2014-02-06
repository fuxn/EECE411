package Interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

import Utilities.Message;

public interface ChatRoomInterface extends Remote {
	public boolean register(ChatUserInterface client) throws RemoteException;
	public boolean unregister(ChatUserInterface client) throws RemoteException;
    public boolean postMessage(MessageInterface message) throws RemoteException;
}
