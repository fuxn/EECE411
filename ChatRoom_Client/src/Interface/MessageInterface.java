package Interface;

import java.rmi.Remote;

public interface MessageInterface extends Remote {

	public ChatUserInterface getClient();

	public String getMessage();

}
