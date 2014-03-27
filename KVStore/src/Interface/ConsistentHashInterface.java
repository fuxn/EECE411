package Interface;

import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;

public interface ConsistentHashInterface {

	public byte[] put(Integer key, byte[] value) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException,
			OutOfSpaceException;

	public byte[] get(Integer key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException;

	public byte[] remove(Integer key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException;

	public void handleAnnouncedFailure()
			throws InternalKVStoreFailureException;

	public void handleNeighbourAnnouncedFailure(byte[] key, byte[] value)
			throws InexistentKeyException, InternalKVStoreFailureException,
			InvalidKeyException, OutOfSpaceException;

	public void execInternal(Socket socket, int command, byte[] key,
			byte[] value);

	public void execHashOperation(Selector selector, SelectionKey handle,
			int command, byte[] key, byte[] value);

}
