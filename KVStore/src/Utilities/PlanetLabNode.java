package Utilities;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import Utilities.Message.MessageUtilities;
import Exception.InexistentKeyException;
import Exception.OutOfSpaceException;

public class PlanetLabNode {

	private String hostName;
	private String ipAddress;
	private ConcurrentHashMap<String, String> values = new ConcurrentHashMap<String, String>();

	public PlanetLabNode(String ipAddress, String hostName) {
		this.hostName = hostName;
		this.ipAddress = ipAddress;
	}

	public byte[] put(String key, String value) throws InexistentKeyException,
			OutOfSpaceException {
		if (this.values.size() > 40000)
			throw new OutOfSpaceException();

		try {
			this.values.put(key, value);
		} catch (OutOfMemoryError e) {
			throw new OutOfSpaceException();
		}

		for (String index : values.keySet()) {
			System.out.println("key: " + index + " value: "
					+ this.values.get(index));
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public byte[] get(String key) throws InexistentKeyException {
		if (this.isInexistentKey(key))
			throw new InexistentKeyException();

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), this.values.get(key));
	}

	public byte[] remove(String key) throws InexistentKeyException {
		if (this.isInexistentKey(key))
			throw new InexistentKeyException();

		this.values.remove(key);

		if (!this.values.isEmpty()) {
			for (String index : values.keySet()) {
				System.out.println("key: " + index + " value: "
						+ values.get(index));
			}
		}

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public Map<String, String> getKeys(String toKey) {
		Map<String, String> keys = new HashMap<String, String>();
		for (String key : this.values.keySet()) {
			if (key.hashCode() <= toKey.hashCode())
				keys.put(key, this.values.get(key));
		}
		return keys;
	}

	public Map<String, String> getKeys() {
		return this.values;
	}

	public String getHostName() {
		return this.hostName;
	}

	public String getIpAddress() {
		return this.ipAddress;
	}

	public void removeAll() {
		this.values.clear();
	}

	private boolean isInexistentKey(String key) {
		return (!this.values.containsKey(key));
	}

}
