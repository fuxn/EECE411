package KVStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import Utilities.ErrorEnum;
import Utilities.Message.Message;
import Utilities.Message.MessageUtilities;
import Exception.InexistentKeyException;
import Exception.OutOfSpaceException;

public class PlanetLabNode {

	private ConcurrentHashMap<Integer, byte[]> values = new ConcurrentHashMap<Integer, byte[]>();
	private ConcurrentHashMap<Integer, Integer> version = new ConcurrentHashMap<Integer, Integer>();

	public byte[] put(Integer key, byte[] value) throws OutOfSpaceException {
		if (this.values.size() > 40000)
			throw new OutOfSpaceException();

		try {
			this.values.put(key, value);
			if (!this.version.contains(key))
				this.version.put(key, 0);
			else
				this.version.put(key, this.version.get(key) + 1);

		} catch (OutOfMemoryError e) {
			throw new OutOfSpaceException();
		}

		return MessageUtilities
				.formateReplyMessage(ErrorEnum.SUCCESS.getCode());
	}

	public boolean put_Local(Integer key, byte[] value)
			throws OutOfSpaceException {
		if (this.values.size() > 40000)
			throw new OutOfSpaceException();

		try {
			this.values.put(key, value);
			if (!this.version.contains(key))
				this.version.put(key, 0);
			else
				this.version.put(key, this.version.get(key) + 1);

		} catch (OutOfMemoryError e) {
			throw new OutOfSpaceException();
		}

		return true;
	}

	public byte[] get(Integer key) {

		byte[] value = this.values.get(key);
		if (value == null)
			return MessageUtilities
					.formateReplyMessage(ErrorEnum.INEXISTENT_KEY.getCode());

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), value);

	}

	public byte[] getReplica(Integer key) throws InexistentKeyException {
		try {
			byte[] value = this.values.get(key);
			Integer version = this.version.get(key);
			return MessageUtilities.formateReplyMessage(
					ErrorEnum.SUCCESS.getCode(), value, version);
		} catch (NullPointerException e) {
			throw new InexistentKeyException();
		}
	}

	public byte[] remove(Integer key) {
		this.values.remove(key);
		return MessageUtilities
				.formateReplyMessage(ErrorEnum.SUCCESS.getCode());
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

	public void removeAll() {
		this.values.clear();
	}

	private boolean isInexistentKey(Integer key) {
		return (!this.values.containsKey(key));
	}

}
