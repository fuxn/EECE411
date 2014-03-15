package Utilities;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Interface.ConsistentHashInterface;
import Utilities.Message.MessageUtilities;

public class ConsistentHash implements ConsistentHashInterface {

	private LookupService lookupService;
	private int numberOfReplicas;

	public ConsistentHash(int numberOfReplicas, Collection<PlanetLabNode> nodes) {
		this.lookupService = new LookupService(numberOfReplicas, nodes);
		this.numberOfReplicas = numberOfReplicas;
	}

	public byte[] put(String key, String value) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException,
			OutOfSpaceException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		List<PlanetLabNode> nodes = this.lookupService.getNodes(key,
				this.numberOfReplicas);

		PlanetLabNode firstNode = nodes.get(0);
		byte[] reply = this.put(firstNode, key, value);
		nodes.remove(0);

		for (PlanetLabNode node : nodes) {
			this.put(node, key, value);
		}

		return reply;

	}

	private byte[] put(PlanetLabNode node, String key, String value)
			throws InexistentKeyException, OutOfSpaceException,
			InternalKVStoreFailureException {

		try {
			String server = InetAddress.getLocalHost().getHostName();
			if (node.getHostName().equals(server)) {
				return node.put(key, value);
			} else {

				return this.lookupService.remoteRequest(1, key.getBytes(),
						value.getBytes(), node.getHostName());
			}

		} catch (UnknownHostException e) {
			throw new InternalKVStoreFailureException();
		}
	}

	public byte[] get(String key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		PlanetLabNode node = this.lookupService.getNode(key);
		try {
			String server = InetAddress.getLocalHost().getHostName();
			if (node.getHostName().equals(server)) {
				return node.get(key);
			} else {
				return this.lookupService.remoteRequest(2, key.getBytes(),
						null, node.getHostName());
			}

		} catch (UnknownHostException e) {
			throw new InternalKVStoreFailureException();
		}

	}

	public byte[] remove(String key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		PlanetLabNode node = this.lookupService.getNode(key);
		try {
			String server = InetAddress.getLocalHost().getHostName();
			if (node.getHostName().equals(server)) {
				return node.remove(key);
			} else {
				return this.lookupService.remoteRequest(3, key.getBytes(),
						null, node.getHostName());
			}

		} catch (UnknownHostException e) {
			throw new InternalKVStoreFailureException();
		}
	}

	public byte[] handleAnnouncedFailure()
			throws InternalKVStoreFailureException {
		try {
			String hostName = InetAddress.getLocalHost().getHostName();

			PlanetLabNode node = this.lookupService.getNodeByHostName(hostName);

			PlanetLabNode nextNode = this.lookupService
					.getNextNodeByHostName(hostName);

			SortedMap<Integer, String> keys = node.getKeys();
			for (Integer key : keys.keySet()) {
				this.lookupService.remoteRequest(
						21,
						MessageUtilities.standarizeMessage(
								new byte[] { key.byteValue() }, 32),
						keys.get(key).getBytes(), nextNode.getHostName());
			}
			node.removeAll();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public byte[] handleNeighbourAnnouncedFailure(String key, String value)
			throws InternalKVStoreFailureException, InexistentKeyException,
			OutOfSpaceException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		try {
			String hostName = InetAddress.getLocalHost().getHostName();

			PlanetLabNode node = this.lookupService.getNodeByHostName(hostName);

			System.out.println("handling neighbour announced failure @ "
					+ hostName);
			System.out.println("adding " + key + " and " + value);

			return node.put(key, value);
		} catch (UnknownHostException e) {
			throw new InternalKVStoreFailureException();
		}
	}

	@Override
	public boolean shutDown() {
		return false;
	}
}
