package Server;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;

public class ChatRoomServer {

	public static void main(String[] argv) {
		try {
			if (System.getSecurityManager() == null) {
				System.setSecurityManager(new RMISecurityManager());
			}

			String serverHostName = InetAddress.getLocalHost().getHostName();
			String serverIpAddress = InetAddress.getLocalHost()
					.getHostAddress();

			ChatRoomImpl chatRoom = new ChatRoomImpl();
			Naming.rebind("rmi://localhost/ChatRoom", chatRoom);

			System.out.println("ChatRoom is running on server : "
					+ serverHostName + " @ " + serverIpAddress);

			chatRoom.waitForMessages();
		} catch (Exception e) {
			System.out.println("ChatRoom Server failed: " + e);
		}
	}

}
