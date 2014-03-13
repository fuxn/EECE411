package KVStore;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

import Utilities.PlanetLabNode;

public class KVStore {

	public static void main(String[] args) throws IOException {

		Collection<PlanetLabNode> nodes = new ArrayList<PlanetLabNode>();
		try {
			nodes.add(new PlanetLabNode(InetAddress.getLocalHost()
					.getHostName()));
		} catch (UnknownHostException e) {
		}

		ProtocolImpl protocol = new ProtocolImpl(nodes);
		protocol.startServer();

		// server wait for incoming requests;
		
		
	}
}
