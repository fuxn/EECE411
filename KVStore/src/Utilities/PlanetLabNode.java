package Utilities;

import java.util.SortedMap;
import java.util.TreeMap;

import Utilities.Message.MessageUtilities;
import Exception.InexistentKeyException;
import Exception.OutOfSpaceException;

public class PlanetLabNode {

	private String hostName;
	private SortedMap<Integer, String> values = new TreeMap<Integer, String>();
	
	public PlanetLabNode(String hostName){
		this.hostName = hostName;
	}

	public  synchronized byte[] put(String key, String value) throws InexistentKeyException,
			OutOfSpaceException {
		if (this.values.size() > 40000)
			throw new OutOfSpaceException();

		try {
			this.values.put(key.hashCode(), value);
		} catch (OutOfMemoryError e) {
			throw new OutOfSpaceException();
		}
		for (Integer index : values.keySet()) {
			System.out
					.println("key: " + index + " value: " + values.get(index));
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public  synchronized byte[] get(String key) throws InexistentKeyException {
		if (this.isInexistentKey(key))
			throw new InexistentKeyException();

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), this.values.get(key.hashCode()));
	}

	public  synchronized byte[] remove(String key) throws InexistentKeyException {
		if (this.isInexistentKey(key))
			throw new InexistentKeyException();

		this.values.remove(new String(key).hashCode());

		if (!this.values.isEmpty()) {
			for (Integer index : values.keySet()) {
				System.out.println("key: " + index + " value: "
						+ values.get(index));
			}
		}

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public SortedMap<Integer, String> getKeys() {
		return this.values;
	}
	
	public String getHostName(){
		return this.hostName;
	}

	public void removeAll() {
		this.values.clear();
	}


	private boolean isInexistentKey(String key) {
		return (!this.values.containsKey(key.hashCode()));
	}

}
