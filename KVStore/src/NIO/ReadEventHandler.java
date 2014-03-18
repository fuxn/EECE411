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

	public ReadEventHandler(Selector demultiplexer, Collection<String> nodes) {
		this.selector = demultiplexer;
		this.cHash = new ConsistentHash(1, nodes);
		threadPool = new ThreadPool(maxThreads, maxTasks);
	}

	@Override
	public void handleEvent(SelectionKey handle) throws Exception {
		this.socketChannel = (SocketChannel) handle.channel();

		// Read data from client
		this.socketChannel.read(commandBuffer);
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

		threadPool.execute(new Processor(handle, c, key, value));

		// execAndHandOff(this.socketChannel, c, key, value);

	}

	synchronized void execAndHandOff(SelectionKey handle, int command,
			String key, String value) {

		System.out.println("executing command " + command + " key " + key
				+ " value " + value);
		byte[] replyMessage;

		try {
			if (command == 1)
				replyMessage = cHash.put(key, value);
			else if (command == 2)
				replyMessage = cHash.get(key);
			else if (command == 3)
				replyMessage = cHash.remove(key);
			else if (command == 4)
				replyMessage = cHash.handleAnnouncedFailure();
			else if (command == 21)
				replyMessage = cHash
						.handleNeighbourAnnouncedFailure(key, value);
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
		}

		handle.interestOps(SelectionKey.OP_WRITE);
		handle.attach(ByteBuffer.wrap(replyMessage));

		this.selector.wakeup();
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
			execAndHandOff(this.handle, this.command, this.key, this.value);
		}

	}

}