package KVStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import Utilities.PlanetLabNode;

public class KVStore {

	public static void main(String[] args) throws IOException {

		Collection<PlanetLabNode> nodes = new ArrayList<PlanetLabNode>();

		nodes.add(new PlanetLabNode("planetlab2.cs.ubc.ca"));
		nodes.add(new PlanetLabNode("planetlab1.cs.ubc.ca"));
		nodes.add(new PlanetLabNode("planetlab2.cs.stevens-tech.edu"));
		nodes.add(new PlanetLabNode("planetlab-4.eecs.cwru.edu"));

		ProtocolImpl protocol = new ProtocolImpl(nodes);
		protocol.startServer();

		// server wait for incoming requests;
	}
}
