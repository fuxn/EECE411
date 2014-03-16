package KVStore;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

import NIO.ReactorInitiator;
import Utilities.PlanetLabNode;

public class KVStore {

	public static void main(String[] args) throws IOException {
		String localHostName = InetAddress.getLocalHost().getHostName();
		Collection<String> nodes = new ArrayList<String>();

		/*
		 * nodes.add("planetlab2.cs.ubc.ca"); nodes.add("planetlab1.cs.ubc.ca");
		 * nodes.add("planetlab2.cs.stevens-tech.edu");
		 * nodes.add("planetlab-4.eecs.cwru.edu");
		 */
		nodes.add(localHostName);
		System.out.println(localHostName);

		/*
		 * ProtocolImpl protocol = new ProtocolImpl(nodes);
		 * protocol.startServer();
		 */

		try {
			new ReactorInitiator().initiateReactiveServer(localHostName, nodes);
		} catch (Exception e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

		// server wait for incoming requests;
	}
}
