package Utilities;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Exception.UnrecognizedCommandException;
import Interface.ConsistentHashInterface;
import NIO_Client.ClientDispatcher;
import NIO_Client.ConnectionEventHandler;
import NIO_Client.ReadReplyEventHandler;
import NIO_Client.WriteRequestEventHandler;
import Utilities.Message.MessageUtilities;

public class ConsistentHash implements ConsistentHashInterface {

	private LookupService lookupService;
	private int numberOfReplicas;
	private PlanetLabNode local;

	public ConsistentHash(int numberOfReplicas, Collection<String> nodes) {
		this.lookupService = new LookupService(numberOfReplicas, nodes);
		this.numberOfReplicas = numberOfReplicas;

		try {
			this.local = new PlanetLabNode(InetAddress.getLocalHost()
					.getHostName());

		} catch (UnknownHostException e) {

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public byte[] put(String key, String value) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException,
			OutOfSpaceException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		List<String> nodes = this.lookupService.getNodes(key,
				this.numberOfReplicas);

		String firstNode = nodes.get(0);
		byte[] reply = this.put(firstNode, key, value);
		nodes.remove(0);

		for (String node : nodes) {
			this.put(node, key, value);
		}

		return reply;

	}

	private byte[] put(String node, String key, String value)
			throws InexistentKeyException, OutOfSpaceException,
			InternalKVStoreFailureException {

		return this.local.put(key, value);

	}

	public byte[] get(String key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		return this.local.get(key);

	}

	public byte[] remove(String key) throws InexistentKeyException,
			InternalKVStoreFailureException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		return this.local.remove(key);

	}

	public byte[] handleAnnouncedFailure()
			throws InternalKVStoreFailureException {

		String nextNode = this.lookupService.getNextNodeByHostName(this.local
				.getHostName());

		Map<String, String> keys = this.local.getKeys();
		for (String key : keys.keySet()) {
			try {
				this.connectRemoteServer(nextNode, null, null, MessageUtilities
						.requestMessage(21, MessageUtilities.standarizeMessage(
								key.getBytes(), 32), keys.get(key).getBytes()));
			} catch (Exception e) {
				return MessageUtilities.formateReplyMessage(
						ErrorEnum.INTERNAL_FAILURE.getCode(), null);
			}

		}
		this.local.removeAll();

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public byte[] handleNeighbourAnnouncedFailure(String key, String value)
			throws InternalKVStoreFailureException, InexistentKeyException,
			OutOfSpaceException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		System.out.println("handling neighbour announced failure @ "
				+ this.local.getHostName());
		System.out.println("adding " + key + " and " + value);

		return this.local.put(key, value);

	}

	@Override
	public boolean shutDown() {
		return false;
	}

	public void exec(Selector selector, SelectionKey handle, int command,
			String key, String value) {

		try {
			String node = this.lookupService.getNode(key);

			if (!node.equals(this.local.getHostName())) {

				System.out.println("Remote Requesting");

				this.connectRemoteServer(node, selector, handle,
						MessageUtilities.requestMessage(command,
								key.getBytes(), value.getBytes()));

				

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		byte[] replyMessage = null;
		try {
			if (command == 1)
				replyMessage = this.local.put(key, value);
			else if (command == 2)
				replyMessage = this.local.get(key);
			else if (command == 3)
				replyMessage = this.local.remove(key);
			else if (command == 4)
				replyMessage = this.handleAnnouncedFailure();
			else if (command == 21)
				replyMessage = this.handleNeighbourAnnouncedFailure(key, value);
			else
				throw new UnrecognizedCommandException();

		} catch (InexistentKeyException ex) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INEXISTENT_KEY.getCode(), null);
		} catch (UnrecognizedCommandException uc) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.UNRECOGNIZED_COMMAND.getCode(), null);
		} catch (InternalKVStoreFailureException internalException) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INTERNAL_FAILURE.getCode(), null);
		} catch (InvalidKeyException invalideKeyException) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INVALID_KEY.getCode(), null);
		} catch (OutOfSpaceException e) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.OUT_OF_SPACE.getCode(), null);
		} catch (Exception e) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INTERNAL_FAILURE.getCode(), null);
		}

		handle.interestOps(SelectionKey.OP_WRITE);
		handle.attach(ByteBuffer.wrap(replyMessage));

		selector.wakeup();

	}

	private void connectRemoteServer(String host, Selector selector,
			SelectionKey handle, ByteBuffer message) throws Exception {
		SocketChannel client;
		try {
			client = SocketChannel.open();

			client.configureBlocking(false);
			client.connect(new InetSocketAddress(host, 4560));
			ClientDispatcher.registerChannel(SelectionKey.OP_CONNECT, client,
					selector, handle, message);
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
