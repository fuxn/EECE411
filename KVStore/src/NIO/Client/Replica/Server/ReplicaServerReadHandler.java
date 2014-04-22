package NIO.Client.Replica.Server;

import Exception.SystemOverloadException;
import KVStore.ConsistentHash;
import NIO.EventHandler;
import Utilities.CommandEnum;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Thread.ThreadPool;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class ReplicaServerReadHandler implements EventHandler {
	private Selector selector;
	private SocketChannel socketChannel;
	private static int maxThreads = 60;
	private static int maxTasks = 40000;
	private static ThreadPool threadPool;
	private ByteBuffer commandBuffer = ByteBuffer.allocate(1);
	private ByteBuffer keyBuffer = ByteBuffer.allocate(32);
	private ByteBuffer valueBuffer = ByteBuffer.allocate(1024);
	private ConsistentHash cHash;

	public ReplicaServerReadHandler(Selector demultiplexer, ConsistentHash cHash) {
		this.selector = demultiplexer;
		this.cHash = cHash;
		threadPool = new ThreadPool(maxThreads, maxTasks);
	}

	public synchronized void handleEvent(SelectionKey handle) throws Exception {
		this.socketChannel = ((SocketChannel) handle.channel());

		this.socketChannel.read(this.commandBuffer);
		this.commandBuffer.flip();
		int c = this.commandBuffer.array()[0];
		byte[] key = null;
		byte[] value = null;

		if (MessageUtilities.isCheckRequestKey(c)) {
			this.socketChannel.read(this.keyBuffer);
			this.keyBuffer.flip();
			key = this.keyBuffer.array();
		}

		if (MessageUtilities.isCheckRequestValue(c)) {
			this.socketChannel.read(this.valueBuffer);
			this.valueBuffer.flip();
			value = this.valueBuffer.array();
		}

		System.out.println("Replica Server reading " + c);
		try {
			threadPool.execute(new Processor(handle, this.socketChannel, c,
					key, value));
		} catch (SystemOverloadException e) {
			handle.attach(ByteBuffer.wrap(MessageUtilities
					.formateReplyMessage(ErrorEnum.OUT_OF_SPACE.getCode())));
			handle.interestOps(4);
			this.selector.wakeup();
		}

		this.valueBuffer.clear();
		this.keyBuffer.clear();
		this.commandBuffer.clear();
	}

	public void process(SelectionKey handle, SocketChannel socketChannel,
			int command, byte[] key, byte[] value) {
		System.out.println(command + " " + Arrays.hashCode(key) + " " + value);

		if (command == CommandEnum.PUT_REPLICA.getCode())
			this.cHash.putReplica(this.selector, handle, key, value);
		else if (command == CommandEnum.GET_REPLICA.getCode())
			this.cHash.getReplica(this.selector, handle, key, value);
		else if (command == CommandEnum.DELETE_REPLICA.getCode())
			this.cHash.removeReplica(this.selector, handle, key, value);
		else
			ReplicaServerDispatcher.response(handle, MessageUtilities
					.formateReplyMessage(ErrorEnum.UNRECOGNIZED_COMMAND
							.getCode()));
	}

	class Processor implements Runnable {
		private int command;
		private byte[] key;
		private byte[] value;
		private SelectionKey handle;
		private SocketChannel socketChannel;

		public Processor(SelectionKey handle, SocketChannel socketChannel,
				int command, byte[] key, byte[] value) {
			this.command = command;
			this.key = key;
			this.value = value;
			this.handle = handle;
			this.socketChannel = socketChannel;
		}

		public void run() {
			ReplicaServerReadHandler.this.process(this.handle,
					this.socketChannel, this.command, this.key, this.value);
		}
	}
}