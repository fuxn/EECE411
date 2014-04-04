package NIO.Client;

import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import KVStore.ConsistentHash;
import NIO.EventHandler;
import Utilities.CommandEnum;
import Utilities.Message.MessageUtilities;
import Utilities.Message.RemoteMessage;

public class ClientDispatcher implements Runnable {
	private Map<Integer, EventHandler> registeredHandlers = new ConcurrentHashMap<Integer, EventHandler>();
	private static Selector demultiplexer;
	private static boolean stop = false;

	final static ReentrantLock selectorLock = new ReentrantLock();

	public ClientDispatcher() throws Exception {
		demultiplexer = Selector.open();
	}

	public void registerEventHandler(int eventType, EventHandler eventHandler) {
		this.registeredHandlers.put(Integer.valueOf(eventType), eventHandler);
	}

	public void registerChannel(int eventType, SelectableChannel channel)
			throws Exception {
		channel.register(demultiplexer, eventType);
	}

	public static void registerChannel(int eventType,
			SelectableChannel channel, SelectionKey serverHandle,
			ByteBuffer message,String coord) throws Exception {
		selectorLock.lock();
		try {
			demultiplexer.wakeup();
			channel.register(demultiplexer, SelectionKey.OP_CONNECT,
					new RemoteMessage(serverHandle,null, message,coord));

		} finally {
			selectorLock.unlock();
		}
	}

	public static Selector getDemultiplexer() {
		return demultiplexer;
	}

	public void run() {
		try {
			while (!stop) { // Loop indefinitely

				selectorLock.lock();
				selectorLock.unlock();

				demultiplexer.select();

				Set<SelectionKey> readyHandles = demultiplexer.selectedKeys();

				Iterator<SelectionKey> handleIterator = readyHandles.iterator();

				while (handleIterator.hasNext()) {
					SelectionKey handle = handleIterator.next();

					if (handle.isValid() && handle.isConnectable()) {
						EventHandler handler = (EventHandler) this.registeredHandlers
								.get(SelectionKey.OP_CONNECT);
						
						handler.handleEvent(handle);
						handleIterator.remove();
					}

					if (handle.isValid() && handle.isWritable()) {
						EventHandler handler = (EventHandler) this.registeredHandlers
								.get(SelectionKey.OP_WRITE);
						handler.handleEvent(handle);
						handleIterator.remove();
					}

					if (handle.isValid() && handle.isReadable()) {
						EventHandler handler = (EventHandler) this.registeredHandlers
								.get(SelectionKey.OP_READ);
						handler.handleEvent(handle);
						handleIterator.remove();
					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void stop(){
		stop = true;
	}

}
