package NIO;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Exception.UnrecognizedCommandException;
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

		String key = MessageUtilities.checkRequestKey(c, this.socketChannel,
				this.keyBuffer);
		String value = MessageUtilities.checkRequestValue(c,
				this.socketChannel, this.valueBuffer);

		commandBuffer.clear();
		keyBuffer.clear();
		valueBuffer.clear();

		// threadPool.execute(new Processor(handle, c, key, value));

		this.process(handle, c, key, value);

		// execAndHandOff(this.socketChannel, c, key, value);

	}

	private void process(final SelectionKey handle, final int command,
			final String key, final String value) {
		if (command == 1 || command == 2 || command == 3) {
			(new Thread(new Runnable() {
				@Override
				public void run() {
					cHash.execHashOperation(selector, handle, command, key,
							value);
				}
			})).start();
		} else {
			(new Thread(new Runnable() {
				@Override
				public void run() {
					cHash.execInternal(selector, handle, command, key, value);
				}
			})).start();
		}
	}

	class Processor implements Runnable {
		private int command;
		private String key;
		private String value;
		private SelectionKey handle;

		public Processor(SelectionKey handle, int command, String key,
				String value) {
			this.command = command;
			this.key = key;
			this.value = value;
			this.handle = handle;
		}

		@Override
		public void run() {
			/*
			 * cHash.exec(selector, this.handle, this.command, this.key,
			 * this.value);
			 */
		}

	}

}