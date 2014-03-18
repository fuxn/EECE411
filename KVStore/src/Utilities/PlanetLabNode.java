package Utilities;

import java.util.HashMap;
import java.util.Map;
import Utilities.Message.MessageUtilities;
import Exception.InexistentKeyException;
import Exception.OutOfSpaceException;

public class PlanetLabNode {

	private String hostName;
	private Map<String, String> values = new HashMap<String, String>();
	
	public PlanetLabNode(String hostName){
		this.hostName = hostName;
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
			System.out
					.println("key: " + index + " value: " + this.values.get(index));
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

	public Map<String, String> getKeys() {
		return this.values;
	}
	
	public String getHostName(){
		return this.hostName;
	}

	public void removeAll() {
		this.values.clear();
	}


	private boolean isInexistentKey(String key) {
		return (!this.values.containsKey(key));
	}

}
