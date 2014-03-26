package NIO_Gossip;

import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;

import Exception.SystemOverloadException;
import Interface.ConsistentHashInterface;
import KVStore.Chord;
import NIO.EventHandler;
import Utilities.ConsistentHash;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Thread.ThreadPool;

public class ReadGossipHandler implements EventHandler {
	private Selector selector;
	private SocketChannel socketChannel;

	private ByteBuffer commandBuffer = ByteBuffer.allocate(1);
	private ByteBuffer keyBuffer = ByteBuffer.allocate(32);
	private ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

	private static int maxThreads = 5;
	private static int maxTasks = 40000;
	private static ThreadPool threadPool;

	private ConsistentHashInterface cHash;

	public ReadGossipHandler(Selector demultiplexer) {
		this.selector = demultiplexer;
		threadPool = new ThreadPool(maxThreads, maxTasks);
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		this.socketChannel = (SocketChannel) handle.channel();

		int byteReceived = 0;
		while (byteReceived != 1) {
			byteReceived = this.socketChannel.read(commandBuffer);
		}
		commandBuffer.flip();

		byte[] command = new byte[commandBuffer.limit()];
		commandBuffer.get(command);

		int c = command[0];

		MessageUtilities.checkRequestKey(c, this.socketChannel, this.keyBuffer);

		this.keyBuffer.flip();
		byte[] key = new byte[this.keyBuffer.limit()];
		this.keyBuffer.get(key);

		MessageUtilities.checkRequestValue(c, this.socketChannel,
				this.valueBuffer);

		this.valueBuffer.flip();
		byte[] value = new byte[this.valueBuffer.limit()];
		this.valueBuffer.get(value);

		commandBuffer.clear();
		keyBuffer.clear();
		valueBuffer.clear();

		// threadPool.execute(new Processor(handle, c, key, value));

		try {
			threadPool.execute(new Processor(handle, c, key, value));
		} catch (SystemOverloadException e) {
			handle.attach(ByteBuffer.wrap(MessageUtilities.formateReplyMessage(
					ErrorEnum.OUT_OF_SPACE.getCode(), null)));
			handle.interestOps(SelectionKey.OP_WRITE);
			selector.wakeup();
		}

		// execAndHandOff(this.socketChannel, c, key, value);

	}

	class Processor implements Runnable {
		private int command;
		private byte[] key;
		private byte[] value;
		private SelectionKey handle;

		public Processor(SelectionKey handle, int command, byte[] key,
				byte[] value) {
			this.command = command;
			this.key = key;
			this.value = value;
			this.handle = handle;
		}

		@Override
		public void run() {
			cHash.execInternal(selector, handle, socketChannel, command, key, value);
		}

	}
}
