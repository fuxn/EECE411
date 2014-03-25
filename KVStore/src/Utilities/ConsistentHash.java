package Utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Exception.UnrecognizedCommandException;
import Interface.ConsistentHashInterface;
import KVStore.Chord;
import NIO.Dispatcher;
import NIO_Client.ClientDispatcher;
import Utilities.Message.MessageUtilities;
import Utilities.Message.Requests;

public class ConsistentHash implements ConsistentHashInterface {

	private ChordTopologyService topologyService;
	private int numberOfReplicas;
	private PlanetLabNode local;

	public ConsistentHash(int numberOfReplicas, Chord chord) {
		this.topologyService = new ChordTopologyService(chord);
		this.numberOfReplicas = numberOfReplicas;

		try {
			String localHost = InetAddress.getLocalHost().getHostName();
			String ipAddress = InetAddress.getLocalHost().getHostAddress();
			this.local = new PlanetLabNode(ipAddress, localHost);
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

		List<String> nodes = this.topologyService.getNodes(key,
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
		System.out.println("handleing failure/..");
		Map<String, String> keys = this.local.getKeys();

		String nextNode = this.topologyService.getNextNodeByHostName(this.local
				.getHostName());

		for (String key : keys.keySet()) {
			try {
				this.topologyService.handleNodeLeaving(this.local
						.getIpAddress());
				this.remoteRequest(
						CommandEnum.HANDLE_ANNOUNCED_FAILURE.getCode(),
						MessageUtilities.standarizeMessage(key.getBytes(), 32),
						keys.get(key).getBytes(), nextNode);
			} catch (Exception e) {
				throw new InternalKVStoreFailureException();
			}
		}
		this.local.removeAll();

		this.announceDataSent(nextNode, this.local.getIpAddress(),
				this.local.getHostName());
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public byte[] announceDataSent(String remoteHost, String ipAddress,
			String localHost) throws InternalKVStoreFailureException {
		try {
			return this
					.remoteRequest(
							CommandEnum.DATA_SENT.getCode(),
							MessageUtilities.standarizeMessage(
									ipAddress.getBytes(), 32),
							MessageUtilities.standarizeMessage(
									localHost.getBytes(), 1024), remoteHost);
		} catch (InvalidKeyException e) {
			throw new InternalKVStoreFailureException();
		}
	}

	public void announceLeaving(String ipAddress, String hostName)
			throws InternalKVStoreFailureException {
		System.out.println("cHash announce leaving :" + hostName);
		List<String> randomNodes = this.topologyService.getRandomNodes(
				hostName, 3);

		System.out.println(randomNodes);

		for (String node : randomNodes) {
			try {
				if (node.equals(this.local.getHostName()))
					continue;
				this.remoteRequest(CommandEnum.ANNOUNCE_LEAVING.getCode(),
						MessageUtilities.standarizeMessage(
								ipAddress.getBytes(), 32), MessageUtilities
								.standarizeMessage(hostName.getBytes(), 1024),
						node);
			} catch (Exception e) {
				if (randomNodes.indexOf(node) != randomNodes.size() - 1)
					continue;
			}
		}
	}

	public byte[] announceJoining(String ipAddress, String hostName)
			throws InternalKVStoreFailureException {
		List<String> randomNodes = this.topologyService.getRandomNodes(
				hostName, 3);
		for (String node : randomNodes) {
			try {
				if (node.equals(this.local.getHostName()))
					continue;
				System.out.println("announce joining to host:" + node);
				this.remoteRequest(CommandEnum.ANNOUNCE_JOINING.getCode(),
						MessageUtilities.standarizeMessage(
								ipAddress.getBytes(), 32), MessageUtilities
								.standarizeMessage(hostName.getBytes(), 1024),
						node);
			} catch (Exception e) {
				if (randomNodes.indexOf(node) != randomNodes.size() - 1)
					continue;
				else
					return MessageUtilities.formateReplyMessage(
							ErrorEnum.INTERNAL_FAILURE.getCode(), null);
			}
		}
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public void handleNeighbourAnnouncedFailure(String key, String value)
			throws InternalKVStoreFailureException, InexistentKeyException,
			OutOfSpaceException, InvalidKeyException {
		if (key.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		System.out.println("handling neighbour announced failure @ "
				+ this.local.getHostName());
		System.out.println("adding " + key + " and " + value);

		this.local.put(key, value);
	}

	public void handleNeighbourDataSent(String ipAddress, String hostName)
			throws InternalKVStoreFailureException {

		this.handleAnnouncedLeaving(ipAddress, hostName);
	}

	public byte[] handleNeighbourAnnouncedJoining(String ipAddress,
			String hostName) throws InternalKVStoreFailureException,
			InexistentKeyException, OutOfSpaceException, InvalidKeyException {
		if (hostName.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		System.out.println("handling neighbour announced joining @ "
				+ this.local.getHostName());

		this.handleAnnouncedJoining(ipAddress, hostName);
		Map<String, String> keys = this.local.getKeys(hostName);
		try {
			for (String key : keys.keySet()) {
				System.out.println("sending " + key);

				this.remoteRequest(CommandEnum.PUT.getCode(), key.getBytes(),
						keys.get(key).getBytes(), hostName);
			}
		} catch (Exception e) {
			return MessageUtilities.formateReplyMessage(
					ErrorEnum.INTERNAL_FAILURE.getCode(), null);
		}

		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	public void handleAnnouncedLeaving(String ipAddress, String hostName)
			throws InternalKVStoreFailureException {
		System.out.println("cHash handleAnnouncedLeaving " + hostName);

		if (this.topologyService.isNodeExist(hostName)) {

			this.topologyService.handleNodeLeaving(ipAddress);
			System.out.println("removing node from local chord : " + hostName);
			this.announceLeaving(ipAddress, hostName);
			System.out.println("broadcast leaving : " + hostName);
		}
	}

	public byte[] handleAnnouncedJoining(String ipAddress, String hostName)
			throws InternalKVStoreFailureException, InexistentKeyException,
			OutOfSpaceException, InvalidKeyException {
		if (ipAddress.length() != 32)
			throw new InvalidKeyException("Illegal Key Size.");

		if (!this.topologyService.isNodeExist(ipAddress)) {
			System.out.println("announcing joing " + hostName);
			this.announceJoining(ipAddress, hostName);
			this.topologyService.handleNodeJoining(ipAddress, hostName);

			if (this.topologyService.isSuccessor(this.local.getHostName(),
					hostName))
				this.handleNeighbourAnnouncedJoining(ipAddress, hostName);

			return MessageUtilities.formateReplyMessage(
					ErrorEnum.SUCCESS.getCode(), null);
		} else {
			System.out.println("already exist");
			return MessageUtilities.formateReplyMessage(
					ErrorEnum.SUCCESS.getCode(), null);
		}
	}

	public void execInternal(Selector selector, final SelectionKey handle,
			int command, String key, String value) {
		byte[] replyMessage = null;

		try {
			if (command == CommandEnum.ANNOUNCE_FAILURE.getCode()) {

				try {
					replyMessage = handleAnnouncedFailure();
					handle.attach(new Requests(CommandEnum.ANNOUNCE_FAILURE,
							ByteBuffer.wrap(replyMessage)));
					handle.interestOps(SelectionKey.OP_WRITE);

					Dispatcher.getDemultiplexer().wakeup();
				} catch (InternalKVStoreFailureException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} else if (command == CommandEnum.HANDLE_ANNOUNCED_FAILURE
					.getCode()) {
				this.handleNeighbourAnnouncedFailure(key, value);
				handle.cancel();
				return;
			} else if (command == CommandEnum.ANNOUNCE_LEAVING.getCode()) {
				this.handleAnnouncedLeaving(key, value);
				handle.cancel();
				return;
			} else if (command == CommandEnum.ANNOUNCE_JOINING.getCode())
				replyMessage = this.handleAnnouncedJoining(key, value);
			else if (command == CommandEnum.DATA_SENT.getCode()) {
				this.handleNeighbourDataSent(key, value);
				handle.cancel();
				return;
			} else
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
		
		handle.attach(new Requests(CommandEnum.ANNOUNCE_FAILURE,
				ByteBuffer.wrap(replyMessage)));
		handle.interestOps(SelectionKey.OP_WRITE);

		Dispatcher.getDemultiplexer().wakeup();

	}

	public void execHashOperation(Selector selector, SelectionKey handle,
			int command, String key, String value) {
		byte[] replyMessage = null;

		try {
			String node = this.topologyService.getNode(key);
			if (!node.equals(this.local.getHostName())) {
				replyMessage = this.remoteRequest(command, key.getBytes(),
						value.getBytes(), node);
			} else {
				if (command == CommandEnum.PUT.getCode()) {
					replyMessage = this.local.put(key, value);
				} else if (command == CommandEnum.GET.getCode()) {
					replyMessage = this.local.get(key);
				} else if (command == CommandEnum.DELETE.getCode()) {
					replyMessage = this.local.remove(key);
				} else
					throw new UnrecognizedCommandException();
			}

		} catch (InexistentKeyException ex) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INEXISTENT_KEY.getCode(), null);
		} catch (InternalKVStoreFailureException internalException) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.INTERNAL_FAILURE.getCode(), null);
		} catch (UnrecognizedCommandException uc) {
			replyMessage = MessageUtilities.formateReplyMessage(
					ErrorEnum.UNRECOGNIZED_COMMAND.getCode(), null);
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

		handle.attach(new Requests(CommandEnum.PUT, ByteBuffer
				.wrap(replyMessage)));
		handle.interestOps(SelectionKey.OP_WRITE);
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
					handle, message);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public byte[] remoteRequest(int command, byte[] key, byte[] value,
			String server) throws InternalKVStoreFailureException,
			InvalidKeyException {
		byte[] reply = null;
		try {
			System.out.println("trying remote request to host " + server);
			Socket socket = new Socket(server, 4560);
			System.out.println("Connecting to : " + socket.getInetAddress());
			System.out.println("connecting to server..");

			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			byte[] v = MessageUtilities.formateRequestMessage(command, key,
					value);
			out.write(v);
			out.flush();
			if (MessageUtilities.isCheckReply(command))
				reply = MessageUtilities.checkReplyValue(command, in);
			else
				reply = MessageUtilities.formateReplyMessage(
						ErrorEnum.SUCCESS.getCode(), null);
		} catch (IOException e) {
			throw new InternalKVStoreFailureException();
		}
		System.out.println("chash reply ; " + reply);
		return reply;
	}
}
