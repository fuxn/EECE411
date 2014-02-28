package Utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import Exception.InvalidKeyException;
import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
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
			this.circle.put(node.hashCode(), node);
		}
	}

	private PlanetLabNode getNode(byte[] key) throws InexistentKeyException,
			InternalKVStoreFailureException {
		if (this.circle.isEmpty())
			throw new InternalKVStoreFailureException();

		int hash = key.hashCode();
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
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		PlanetLabNode node = this.getNode(key);
		try {
			String server = InetAddress.getLocalHost().getHostName();
			if (node.getHostName().equals(server)) {
				return node.put(key, value);
			} else {
				return this.remoteRequest(1, key, value, server);
			}

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

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
				return this.remoteRequest(1, key, null, server);
			}

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
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
				return this.remoteRequest(1, key, null, server);
			}

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return node.remove(key);
	}

	private byte[] remoteRequest(int command, byte[] key, byte[] value,
			String server) {
		try {
			Socket socket = new Socket(server, 4560);
			System.out.println("Connecting to : " + socket.getInetAddress());
			System.out.println("connecting to server..");

			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			byte[] v = Message.formateRequestMessage(command, key, value);
			out.write(v);
			out.flush();

			byte[] reply = new byte[1025];
			int bytesRcvd;
			int totalBytesRcvd = 0;
			while (totalBytesRcvd < reply.length) {
				if ((bytesRcvd = in.read(reply, totalBytesRcvd, reply.length
						- totalBytesRcvd)) == -1)
					throw new SocketException("connection close prematurely.");
				totalBytesRcvd += bytesRcvd;
			}
			return reply;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;

	}

}
