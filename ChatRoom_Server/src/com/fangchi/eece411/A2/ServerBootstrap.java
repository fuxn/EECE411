package com.fangchi.eece411.A2;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;

public class ServerBootstrap {

	public static void main(String[] argv) {
		try {
			if (System.getSecurityManager() == null) {
				System.setSecurityManager(new RMISecurityManager());
			}

			String serverHostName = InetAddress.getLocalHost().getHostName();
			String serverIpAddress = InetAddress.getLocalHost()
					.getHostAddress();

			ChatRoomImpl chatRoom = new ChatRoomImpl();
			Naming.rebind("ChatRoom", chatRoom);

			System.out.println("ChatRoom is running on server : "
					+ serverHostName + " @ " + serverIpAddress);

			chatRoom.waitForMessages();
		} catch (Exception e) {
			System.out.println("ChatRoom Server failed: " + e);
		}
	}

}
