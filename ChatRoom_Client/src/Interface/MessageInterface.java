package Interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MessageInterface extends Remote {

	public ChatUserInterface getClient() throws RemoteException;

	public String getMessage() throws RemoteException;

}
