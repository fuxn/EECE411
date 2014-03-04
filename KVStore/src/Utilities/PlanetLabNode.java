package Utilities;

import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

import Exception.InexistentKeyException;
import Exception.OutOfSpaceException;

public class PlanetLabNode {

	private String hostName;
	private SortedMap<Integer, byte[]> values = new TreeMap<Integer, byte[]>();

	public PlanetLabNode(String hostName) {
		this.hostName = hostName;
	}

	public byte[] put(byte[] key, byte[] value) throws InexistentKeyException,
			OutOfSpaceException {
		if (this.values.size() > 40000)
			throw new OutOfSpaceException();

		try {
			this.values.put(new String(key).hashCode(), value);
		} catch (OutOfMemoryError e) {
			throw new OutOfSpaceException();
		}
		for (Integer index : values.keySet()) {
			System.out.println("key: " + index + " value: "
					+ Arrays.toString(values.get(index)));
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public byte[] get(byte[] key) throws InexistentKeyException {
		if (!this.values.containsKey(new String(key).hashCode()))
			throw new InexistentKeyException();

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(),
				this.values.get(new String(key).hashCode()));
	}

	public byte[] remove(byte[] key) throws InexistentKeyException {
		if (!this.values.containsKey(new String(key).hashCode()))
			throw new InexistentKeyException();
		this.values.remove(new String(key).hashCode());
		if (!this.values.isEmpty()) {
			for (Integer index : values.keySet()) {
				System.out.println("key: " + index + " value: "
						+ Arrays.toString(values.get(index)));
			}
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public String getHostName() {
		return this.hostName;
	}

}
