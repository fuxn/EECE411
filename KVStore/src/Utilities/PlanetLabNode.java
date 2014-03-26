package Utilities;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import Utilities.Message.MessageUtilities;
import Exception.InexistentKeyException;
import Exception.OutOfSpaceException;

public class PlanetLabNode {

	private String hostName;
	private ConcurrentHashMap<Integer, byte[]> values = new ConcurrentHashMap<Integer, byte[]>();

	public PlanetLabNode(String hostName) {
		this.hostName = hostName;
	}

	public byte[] put(byte[] key, byte[] value) throws InexistentKeyException,
			OutOfSpaceException {
		if (this.values.size() > 40000)
			throw new OutOfSpaceException();

		try {
			this.values.put(Arrays.hashCode(key), value);
		} catch (OutOfMemoryError e) {
			throw new OutOfSpaceException();
		}

		if (!this.values.isEmpty()) {
			for (Integer index : values.keySet()) {
				System.out.println("key: " + index + " value: "
						+ new String(values.get(index)));
			}
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public byte[] get(byte[] key) throws InexistentKeyException {
		if (this.isInexistentKey(key))
			throw new InexistentKeyException();

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(),
				this.values.get(Arrays.hashCode(key)));
	}

	public byte[] remove(byte[] key) throws InexistentKeyException {
		if (this.isInexistentKey(key))
			throw new InexistentKeyException();

		this.values.remove(Arrays.hashCode(key));

		if (!this.values.isEmpty()) {
			for (Integer index : values.keySet()) {
				System.out.println("key: " + index + " value: "
						+ new String(values.get(index)));
			}
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public Map<Integer, byte[]> getKeys(byte[] toKey) {
		Map<Integer, byte[]> keys = new HashMap<Integer, byte[]>();
		for (Integer key : this.values.keySet()) {
			if (key <= Arrays.hashCode(toKey))
				keys.put(key, this.values.get(key));
		}
		return keys;
	}

	public Map<Integer, byte[]> getKeys() {
		return this.values;
	}

	public String getHostName() {
		return this.hostName;
	}

	public void removeAll() {
		this.values.clear();
	}

	private boolean isInexistentKey(byte[] key) {
		return (!this.values.containsKey(Arrays.hashCode(key)));
	}

}
