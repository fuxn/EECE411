package Utilities;

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

	public byte[] put(Integer key, byte[] value) throws InexistentKeyException,
			OutOfSpaceException {
		if (this.values.size() > 40000)
			throw new OutOfSpaceException();

		try {
			this.values.put(key, value);
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

	public byte[] get(Integer key) throws InexistentKeyException {
		if (this.isInexistentKey(key))
			throw new InexistentKeyException();

		// System.out.println("get :" + this.values.get(key));
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), this.values.get(key));
	}

	public byte[] remove(Integer key) throws InexistentKeyException {
		if (this.isInexistentKey(key))
			throw new InexistentKeyException();

		this.values.remove(key);

		if (!this.values.isEmpty()) {
			for (Integer index : values.keySet()) {
				System.out.println("key: " + index + " value: "
						+ new String(values.get(index)));
			}
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public Map<Integer, byte[]> getKeys(int toKey) {
		Map<Integer, byte[]> keys = new HashMap<Integer, byte[]>();
		for (Integer key : this.values.keySet()) {
			if (key <= toKey)
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

	private boolean isInexistentKey(Integer key) {
		return (!this.values.containsKey(key));
	}

}
