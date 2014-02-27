package Utilities;

import java.util.SortedMap;
import java.util.TreeMap;

import Exception.InexistentKeyException;

public class PlanetLabNode {

	private String hostName;
	private SortedMap<Integer, byte[]> values = new TreeMap<Integer, byte[]>();

	public PlanetLabNode(String hostName) {
		this.hostName = hostName;
	}

	public byte[] put(Object key, byte[] value) throws InexistentKeyException {
		if (!this.values.containsKey(key.hashCode()))
			throw new InexistentKeyException();

		this.values.put(key.hashCode(), value);
		return Message.formateReplyMessage(0, null);
	}

	public byte[] get(Object key) throws InexistentKeyException {
		if (!this.values.containsKey(key.hashCode()))
			throw new InexistentKeyException();

		return Message.formateReplyMessage(0, this.values.get(key.hashCode()));
	}

	public byte[] remove(Object key) throws InexistentKeyException {
		if (!this.values.containsKey(key.hashCode()))
			throw new InexistentKeyException();
		this.values.remove(key.hashCode());
		return Message.formateReplyMessage(0, null);
	}

	public String getHostName() {
		return this.hostName;
	}

}
