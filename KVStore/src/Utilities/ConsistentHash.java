package Utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Interface.ConsistentHashInterface;

public class ConsistentHash implements ConsistentHashInterface {

	private final SortedMap<Integer, PlanetLabNode> circle = new TreeMap<Integer, PlanetLabNode>();
	private final int numberOfReplicas;

	public ConsistentHash(int numberOfReplicas, Collection<PlanetLabNode> nodes) {
		this.numberOfReplicas = numberOfReplicas;
		for (PlanetLabNode node : nodes) {
			this.addNode(node);
		}
	}

	public void addNode(PlanetLabNode node) {
		for (int i = 0; i < this.numberOfReplicas; i++) {
			this.circle.put(node.getHostName().hashCode(), node);
		}
	}

	private PlanetLabNode getNode(byte[] key) throws InexistentKeyException,
			InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		int hash = new String(key).hashCode();
		System.out.println("PlanetLabNode getNode key hashCode : " + hash);
		if (!this.circle.containsKey(hash)) {
			SortedMap<Integer, PlanetLabNode> tailMap = this.circle
					.tailMap(hash);
			hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap
					.firstKey();
		} else
			throw new InexistentKeyException();

		return this.circle.get(hash);
	}

	public byte[] put(byte[] key, byte[] value) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException, OutOfSpaceException {
		if (key.length != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		PlanetLabNode node = this.getNode(key);
		try {
			String server = InetAddress.getLocalHost().getHostName();
			if (node.getHostName().equals(server)) {
				return node.put(key, value);
			} else {
				return this.remoteRequest(1, key, value, node.getHostName());
			}

		} catch (UnknownHostException e) {
			throw new InternalKVStoreFailureException();
		}

	}

	public byte[] get(byte[] key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		PlanetLabNode node = this.getNode(key);
		try {
			String server = InetAddress.getLocalHost().getHostName();
			if (node.getHostName().equals(server)) {
				return node.get(key);
			} else {
				return this.remoteRequest(2, key, null, node.getHostName());
			}

		} catch (UnknownHostException e) {
			throw new InternalKVStoreFailureException();
		}

	}

	public byte[] remove(byte[] key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		PlanetLabNode node = this.getNode(key);
		try {
			String server = InetAddress.getLocalHost().getHostName();
			if (node.getHostName().equals(server)) {
				return node.remove(key);
			} else {
				return this.remoteRequest(3, key, null, node.getHostName());
			}

		} catch (UnknownHostException e) {
			throw new InternalKVStoreFailureException();
		}
	}

	private byte[] remoteRequest(int command, byte[] key, byte[] value,
			String server) throws InternalKVStoreFailureException {
		byte[] reply = null;
		try {
			Socket socket = new Socket(server, 4560);
			System.out.println("Connecting to : " + socket.getInetAddress());
			System.out.println("connecting to server..");

			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			byte[] v = MessageUtilities.formateRequestMessage(command, key, value);
			out.write(v);
			out.flush();

			reply = MessageUtilities.checkReplyValue(command, in);
		} catch (IOException e) {
			throw new InternalKVStoreFailureException();
		}
		return reply;
	}
}
