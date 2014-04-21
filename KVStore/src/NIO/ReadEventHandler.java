package NIO;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import Exception.SystemOverloadException;
import Interface.CommandHandler;
import KVStore.ConsistentHash;
import Utilities.CommandEnum;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Thread.ThreadPool;

public class ReadEventHandler implements EventHandler {

	private Selector selector;
	private SocketChannel socketChannel;

	private static int maxThreads = 60;
	private static int maxTasks = 40000;
	private static ThreadPool threadPool;

	private ByteBuffer commandBuffer = ByteBuffer.allocate(1);
	private ByteBuffer keyBuffer = ByteBuffer.allocate(32);
	private ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

	private ConsistentHash cHash;

	public ReadEventHandler(Selector demultiplexer, ConsistentHash cHash) {
		this.selector = demultiplexer;
		this.cHash = cHash;
		threadPool = new ThreadPool(maxThreads, maxTasks);
	}

	@Override
	public synchronized void handleEvent(SelectionKey handle) throws Exception {
		this.socketChannel = (SocketChannel) handle.channel();

		// Read data from client
		this.socketChannel.read(commandBuffer);
		commandBuffer.flip();
		int c = commandBuffer.array()[0];
		byte[] key = null;
		byte[] value = null;

		if (MessageUtilities.isCheckRequestKey(c)) {
			this.socketChannel.read(keyBuffer);
			keyBuffer.flip();
			key = keyBuffer.array();
		}

		if (MessageUtilities.isCheckRequestValue(c)) {
			this.socketChannel.read(valueBuffer);
			valueBuffer.flip();
			value = valueBuffer.array();
		}

		System.out.println("Server reading " + c);

		// threadPool.execute(new Processor(handle, c, key, value));

		try {
			threadPool.execute(new Processor(handle, this.socketChannel, c,
					key, value));
		} catch (SystemOverloadException e) {
			handle.attach(ByteBuffer.wrap(MessageUtilities
					.formateReplyMessage(ErrorEnum.OUT_OF_SPACE.getCode())));
			handle.interestOps(SelectionKey.OP_WRITE);
			selector.wakeup();

		}

		valueBuffer.clear();
		keyBuffer.clear();
		commandBuffer.clear();

		// execAndHandOff(this.socketChannel, c, key, value);

	}

	public void process(final SelectionKey handle,
			final SocketChannel socketChannel, final int command,
			final byte[] key, final byte[] value) {
		System.out.println(command + " " + Arrays.hashCode(key) + " " + value);

		if (ConsistentHash.commandHandlers.containsKey(command)) {
			CommandHandler cmdHandler = ConsistentHash.commandHandlers
					.get(command);
			cmdHandler.executCommand(cHash, selector, handle, key, value);
		} else {
			Dispatcher.response(handle, MessageUtilities
					.formateReplyMessage(ErrorEnum.UNRECOGNIZED_COMMAND
							.getCode()));
		}
	}

	/*
	 * public void process(final SelectionKey handle, final SocketChannel
	 * socketChannel, final int command, final byte[] key, final byte[] value) {
	 * System.out.println(command + " " + Arrays.hashCode(key) + " " + value);
	 * 
	 * if (command == CommandEnum.PUT.getCode()) cHash.put(selector, handle,
	 * key, value); else if (command == CommandEnum.GET.getCode())
	 * cHash.get(selector, handle, key, value); else if (command ==
	 * CommandEnum.DELETE.getCode()) cHash.remove(selector, handle, key, value);
	 * else if (command == CommandEnum.ANNOUNCE_FAILURE.getCode()) {
	 * 
	 * Dispatcher.stopAccept(); Dispatcher.response(handle, MessageUtilities
	 * .formateReplyMessage(ErrorEnum.SUCCESS.getCode()));
	 * 
	 * System.exit(0); } else { Dispatcher.response(handle, MessageUtilities
	 * .formateReplyMessage(ErrorEnum.UNRECOGNIZED_COMMAND .getCode())); } }
	 */

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

		@Override
		public void run() {
			process(handle, socketChannel, command, key, value);
		}

	}

}