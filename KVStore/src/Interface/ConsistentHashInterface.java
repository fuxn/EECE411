package Interface;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;

public interface ConsistentHashInterface {

	public byte[] put(byte[] key, byte[] value) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException,
			OutOfSpaceException;

	public byte[] get(byte[] key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException;

	public byte[] remove(byte[] key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException;
}
