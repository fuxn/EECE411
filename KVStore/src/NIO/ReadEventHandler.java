package NIO;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import Exception.SystemOverloadException;
import Interface.ConsistentHashInterface;
import KVStore.Chord;
import Utilities.ConsistentHash;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Thread.ThreadPool;

public class ReadEventHandler implements EventHandler {

	private Selector selector;
	private SocketChannel socketChannel;

	private ByteBuffer commandBuffer = ByteBuffer.allocate(1);
	private ByteBuffer keyBuffer = ByteBuffer.allocate(32);
	private ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

	private static int maxThreads = 5;
	private static int maxTasks = 40000;
	private static ThreadPool threadPool;

	private ConsistentHashInterface cHash;

	public ReadEventHandler(Selector demultiplexer, Chord chord) {
		this.selector = demultiplexer;
		this.cHash = new ConsistentHash(1, chord);
		threadPool = new ThreadPool(maxThreads, maxTasks);
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		this.socketChannel = (SocketChannel) handle.channel();

		System.out.println("Server channel reading ");
		// Read data from client
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

	public void process(final SelectionKey handle, final int command,
			final byte[] key, final byte[] value) {
		if (command == 1 || command == 2 || command == 3) {
			cHash.execHashOperation(selector, handle, command, key, value);
		} else {
			cHash.execInternal(selector, handle, command, key, value);

		}
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
			process(handle, command, key, value);
		}

	}

}