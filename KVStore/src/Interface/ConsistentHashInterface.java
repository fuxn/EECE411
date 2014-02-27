package Interface;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;

public interface ConsistentHashInterface {

	public byte[] put(byte[] key, byte[] value) throws InexistentKeyException,
			InternalKVStoreFailureException;

	public byte[] get(byte[] key) throws InexistentKeyException,
			InternalKVStoreFailureException;

	public byte[] remove(byte[] key) throws InexistentKeyException,
			InternalKVStoreFailureException;
}
