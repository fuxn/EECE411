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

	public byte[] put(byte[] key, byte[] value) throws InexistentKeyException {
		this.values.put(new String(key).hashCode(), value);
		System.out.println(this.values);
		return Message.formateReplyMessage(0, null);
	}

	public byte[] get(byte[] key) throws InexistentKeyException {
		if (!this.values.containsKey(new String(key).hashCode()))
			throw new InexistentKeyException();

		return Message.formateReplyMessage(0,
				this.values.get(new String(key).hashCode()));
	}

	public byte[] remove(byte[] key) throws InexistentKeyException {
		if (!this.values.containsKey(new String(key).hashCode()))
			throw new InexistentKeyException();
		this.values.remove(new String(key).hashCode());
		System.out.println(this.values);
		return Message.formateReplyMessage(0, null);
	}

	public String getHostName() {
		return this.hostName;
	}

}
