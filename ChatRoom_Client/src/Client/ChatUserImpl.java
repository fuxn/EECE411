package Client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Queue;

import Interface.ChatRoomInterface;
import Interface.ChatUserInterface;
import Utilities.MessageQueue;

public class ChatUserImpl extends UnicastRemoteObject implements
		ChatUserInterface {

	 MessageQueue queue;

	public ChatUserImpl() throws RemoteException {
		super();
	    queue = new MessageQueue();
	}

	@Override
	public boolean broadCastMessage(String message) {
		if (message == null )
			return false;

		System.out.println("Message from Server : " + message);
		queue.enqueue(message);
		return true;
	}
	
	public MessageQueue getMessageQueue(){
		return queue;
	}

}
