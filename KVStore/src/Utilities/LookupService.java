package Utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import Utilities.Message.MessageUtilities;
import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;

public class LookupService {
	private SortedMap<Integer, PlanetLabNode> circle = new TreeMap<Integer, PlanetLabNode>();

	public LookupService(int numberOfReplicas, Collection<PlanetLabNode> nodes) {
		for (PlanetLabNode node : nodes) {
			this.circle.put(node.getHostName().hashCode(), node);
		}
	}

	public PlanetLabNode getNode(String key) throws InexistentKeyException,
			InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		int hash = key.hashCode();
		System.out.println("PlanetLabNode getNode key hashCode : " + hash);
		if (!this.circle.containsKey(hash)) {
			SortedMap<Integer, PlanetLabNode> tailMap = this.circle
					.tailMap(hash);
			hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
					.firstKey();
		}

		return this.circle.get(hash);
	}

	public List<PlanetLabNode> getNodes(String fromKey, int numOfReplicas)
			throws InexistentKeyException, InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		List<PlanetLabNode> nodes = new ArrayList<PlanetLabNode>();

		int hash = fromKey.hashCode();
		System.out.println("PlanetLabNode getNode key hashCode : " + hash);
		if (!this.circle.containsKey(hash)) {
			SortedMap<Integer, PlanetLabNode> tailMap = this.circle
					.tailMap(hash);
			hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
					.firstKey();
		}

		nodes.add(this.circle.get(hash));

		for (int i = 0; i < numOfReplicas - 1; i++) {
			nodes.add(this.getNextNode(hash));
		}

		return nodes;
	}

	private PlanetLabNode getNextNode(int key)
			throws InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		SortedMap<Integer, PlanetLabNode> tailMap = this.circle.tailMap(key);
		int hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
				.firstKey();

		return this.circle.get(hash);
	}

	public PlanetLabNode getNodeByHostName(String hostName)
			throws InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		return this.circle.get(hostName.hashCode());
	}

	public PlanetLabNode getNextNodeByHostName(String hostName)
			throws InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		SortedMap<Integer, PlanetLabNode> tailMap = this.circle
				.tailMap(hostName.hashCode() + 1);
		int hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
				.firstKey();

		return this.circle.get(hash);
	}

	public byte[] remoteRequest(int command, byte[] key, byte[] value,
			String server) throws InternalKVStoreFailureException {
		byte[] reply = null;
		try {
			Socket socket = new Socket(server, 4560);
			System.out.println("Connecting to : " + socket.getInetAddress());
			System.out.println("connecting to server..");

			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			byte[] v = MessageUtilities.formateRequestMessage(command, key,
					value);
			out.write(v);
			out.flush();

			reply = MessageUtilities.checkReplyValue(command, in);
		} catch (IOException e) {
			
			throw new InternalKVStoreFailureException();
		}
		return reply;
	}

}
