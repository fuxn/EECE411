package Interface;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

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

	public byte[] handleAnnouncedFailure()
			throws InternalKVStoreFailureException;

	public void handleNeighbourAnnouncedFailure(byte[] key, byte[] value)
			throws InexistentKeyException, InternalKVStoreFailureException,
			InvalidKeyException, OutOfSpaceException;

	public void execInternal(Selector selector, SelectionKey handle,
			int command, byte[] key, byte[] value);

	public void execHashOperation(Selector selector, SelectionKey handle,
			int command, byte[] key, byte[] value);

}
