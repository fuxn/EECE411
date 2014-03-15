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
	private PlanetLabNode local;

	public ConsistentHash(int numberOfReplicas, Collection<String> nodes) {
		this.lookupService = new LookupService(numberOfReplicas, nodes);
		this.numberOfReplicas = numberOfReplicas;

		try {
			this.local = new PlanetLabNode(InetAddress.getLocalHost()
					.getHostName());
		} catch (UnknownHostException e) {

		}
	}

	public byte[] put(String key, String value) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException,
			OutOfSpaceException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		List<String> nodes = this.lookupService.getNodes(key,
				this.numberOfReplicas);

		String firstNode = nodes.get(0);
		byte[] reply = this.put(firstNode, key, value);
		nodes.remove(0);

		for (String node : nodes) {
			this.put(node, key, value);
		}

		return reply;

	}

	private byte[] put(String node, String key, String value)
			throws InexistentKeyException, OutOfSpaceException,
			InternalKVStoreFailureException {

		if (node.equals(this.local.getHostName())) {
			return this.local.put(key, value);
		} else {

			return this.lookupService.remoteRequest(1, key.getBytes(),
					value.getBytes(), node);
		}

	}

	public byte[] get(String key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		String node = this.lookupService.getNode(key);
		try {
			String server = InetAddress.getLocalHost().getHostName();
			if (node.equals(server)) {
				return this.local.get(key);
			} else {
				return this.lookupService.remoteRequest(2, key.getBytes(),
						null, node);
			}

		} catch (UnknownHostException e) {
			throw new InternalKVStoreFailureException();
		}

	}

	public byte[] remove(String key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		String node = this.lookupService.getNode(key);

		if (node.equals(this.local.getHostName())) {
			return this.local.remove(key);
		} else {
			return this.lookupService.remoteRequest(3, key.getBytes(), null,
					node);
		}

	}

	public byte[] handleAnnouncedFailure()
			throws InternalKVStoreFailureException {

		String nextNode = this.lookupService.getNextNodeByHostName(this.local
				.getHostName());

		SortedMap<Integer, String> keys = this.local.getKeys();
		for (Integer key : keys.keySet()) {
			this.lookupService.remoteRequest(21, MessageUtilities
					.standarizeMessage(new byte[] { key.byteValue() }, 32),
					keys.get(key).getBytes(), nextNode);
		}
		this.local.removeAll();

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public byte[] handleNeighbourAnnouncedFailure(String key, String value)
			throws InternalKVStoreFailureException, InexistentKeyException,
			OutOfSpaceException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		System.out.println("handling neighbour announced failure @ "
				+ this.local.getHostName());
		System.out.println("adding " + key + " and " + value);

		return this.local.put(key, value);

	}

	@Override
	public boolean shutDown() {
		return false;
	}
}
