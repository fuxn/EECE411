package NIO;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import Exception.InternalKVStoreFailureException;
import Exception.SystemOverloadException;
import Interface.ConsistentHashInterface;
import Utilities.CommandEnum;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Message.Requests;
import Utilities.Thread.ThreadPool;

public class ReadEventHandler implements EventHandler {

	private Selector selector;
	private SocketChannel socketChannel;

	private static int maxThreads = 10;
	private static int maxTasks = 40000;
	private static ThreadPool threadPool;

	private ConsistentHashInterface cHash;

	public ReadEventHandler(Selector demultiplexer,
			ConsistentHashInterface cHash) {
		this.selector = demultiplexer;
		this.cHash = cHash;
		threadPool = new ThreadPool(maxThreads, maxTasks);
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		this.socketChannel = (SocketChannel) handle.channel();

		// Read data from client
		ByteBuffer buffer = ByteBuffer.allocate(1);
		this.socketChannel.read(buffer);
		buffer.flip();
		int c = buffer.array()[0];
		byte[] key = null;
		byte[] value = null;

		if (MessageUtilities.isCheckRequestKey(c)) {
			buffer = ByteBuffer.allocate(32);
			this.socketChannel.read(buffer);
			buffer.flip();
			key = buffer.array();
		}

		if (MessageUtilities.isCheckRequestValue(c)) {
			buffer = ByteBuffer.allocate(1024);
			this.socketChannel.read(buffer);
			buffer.flip();
			value = buffer.array();
		}
		buffer.clear();

		// threadPool.execute(new Processor(handle, c, key, value));

		try {
			threadPool.execute(new Processor(handle, this.socketChannel, c,
					key, value));
		} catch (SystemOverloadException e) {
			handle.attach(ByteBuffer.wrap(MessageUtilities.formateReplyMessage(
					ErrorEnum.OUT_OF_SPACE.getCode(), null)));
			handle.interestOps(SelectionKey.OP_WRITE);
			selector.wakeup();

		}

		// execAndHandOff(this.socketChannel, c, key, value);

	}

	public void process(final SelectionKey handle,
			final SocketChannel socketChannel, final int command,
			final byte[] key, final byte[] value) {
		if (command == 1 || command == 2 || command == 3) {
			cHash.execHashOperation(selector, handle, command, key, value);
		} else if (command == 4) {

			Dispatcher.stopAccept();
			handle.attach(ByteBuffer.wrap(MessageUtilities.formateReplyMessage(
					ErrorEnum.SUCCESS.getCode(), null)));
			handle.interestOps(SelectionKey.OP_WRITE);
			Dispatcher.getDemultiplexer().wakeup();
			System.exit(0);

		} else {
			handle.attach(new Requests(CommandEnum.PUT, ByteBuffer
					.wrap(MessageUtilities.formateReplyMessage(
							ErrorEnum.UNRECOGNIZED_COMMAND.getCode(), null))));
			handle.interestOps(SelectionKey.OP_WRITE);

			Dispatcher.getDemultiplexer().wakeup();
		}
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

		@Override
		public void run() {
			process(handle, socketChannel, command, key, value);
		}

	}

}