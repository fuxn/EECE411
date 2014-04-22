package NIO;

import Exception.SystemOverloadException;
import KVStore.ConsistentHash;
import Utilities.CommandEnum;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Thread.ThreadPool;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class ReadEventHandler implements EventHandler {
	private Selector selector;
	private SocketChannel socketChannel;
	private ByteBuffer commandBuffer = ByteBuffer.allocate(1);
	private ByteBuffer keyBuffer = ByteBuffer.allocate(32);
	private ByteBuffer valueBuffer = ByteBuffer.allocate(1024);
	private ConsistentHash cHash;

	public ReadEventHandler(Selector demultiplexer, ConsistentHash cHash) {
		this.selector = demultiplexer;
		this.cHash = cHash;
	}

	public synchronized void handleEvent(SelectionKey handle) throws Exception {
		this.socketChannel = ((SocketChannel) handle.channel());

		byte[] key = null;
		byte[] value = null;
		int c = 0;

		if (this.socketChannel.read(this.commandBuffer) != -1) {

			this.commandBuffer.flip();
			c = this.commandBuffer.array()[0];

		} else {
			handle.cancel();
			this.socketChannel.close();
			return;
		}

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

		System.out.println("Server reading " + c);
		
			KVStore.KVStore.threadPool.execute(new Processor(handle, this.socketChannel, c,
					key, value));
		

		this.valueBuffer.clear();
		this.keyBuffer.clear();
		this.commandBuffer.clear();
	}

	public void process(SelectionKey handle, SocketChannel socketChannel,
			int command, byte[] key, byte[] value) {
		System.out.println(command + " " + Arrays.hashCode(key) + " " + value);

		if (command == CommandEnum.PUT.getCode()) {
			this.cHash.put(this.selector, handle, key, value);
		} else if (command == CommandEnum.GET.getCode()) {
			this.cHash.get(this.selector, handle, key, value);
		} else if (command == CommandEnum.DELETE.getCode()) {
			this.cHash.remove(this.selector, handle, key, value);
		}else if(command == CommandEnum.PUT_COORD.getCode()){
			this.cHash.putCoord(selector, handle, key, value);
		}
		else if (command == CommandEnum.ANNOUNCE_FAILURE.getCode()) {
			Dispatcher.stopAccept();
			Dispatcher.response(handle, MessageUtilities
					.formateReplyMessage(ErrorEnum.SUCCESS.getCode()));

			System.exit(0);
		} else {
			Dispatcher.response(handle, MessageUtilities
					.formateReplyMessage(ErrorEnum.UNRECOGNIZED_COMMAND
							.getCode()));
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

		public void run() {
			ReadEventHandler.this.process(this.handle, this.socketChannel,
					this.command, this.key, this.value);
		}
	}
}