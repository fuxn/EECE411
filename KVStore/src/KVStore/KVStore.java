package KVStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import Utilities.PlanetLabNode;

public class KVStore {

	public static void main(String[] args) throws IOException {

		Collection<String> nodes = new ArrayList<String>();

		nodes.add("planetlab2.cs.ubc.ca");
		nodes.add("planetlab1.cs.ubc.ca");
		nodes.add("planetlab2.cs.stevens-tech.edu");
		nodes.add("planetlab-4.eecs.cwru.edu");

		ProtocolImpl protocol = new ProtocolImpl(nodes);
		protocol.startServer();

		// server wait for incoming requests;
	}
}
