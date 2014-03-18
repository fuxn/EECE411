package Interface;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;

public interface ConsistentHashInterface {

	public byte[] put(String key, String value) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException,
			OutOfSpaceException;

	public byte[] get(String key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException;

	public byte[] remove(String key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException;

	public byte[] handleAnnouncedFailure()
			throws InternalKVStoreFailureException;

	public byte[] handleNeighbourAnnouncedFailure(String key, String value)
			throws InexistentKeyException, InternalKVStoreFailureException,
			InvalidKeyException, OutOfSpaceException;
	
	public void exec(Selector selector, SelectionKey handle, int command,
			String key, String value);

	public boolean shutDown();
}
