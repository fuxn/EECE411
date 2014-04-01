package NIO.Client.Replica;

import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import NIO.EventHandler;
import Utilities.Message.RemoteMessage;

public class ReplicaDispatcher implements Runnable {
	private Map<Integer, EventHandler> registeredHandlers = new ConcurrentHashMap<Integer, EventHandler>();
	private static Map<SelectionKey, SelectionKey> handleQueue = new HashMap<SelectionKey, SelectionKey>();
	private static Map<SelectionKey, Integer> keyQueue = new HashMap<SelectionKey, Integer>();
	public static List<SelectionKey> pendingHandle = new ArrayList<SelectionKey>();
	
	private static Selector demultiplexer;
	private static boolean stop = false;

	final static ReentrantLock selectorLock = new ReentrantLock();

	public ReplicaDispatcher() throws Exception {
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
			SelectableChannel channel, Integer key, ByteBuffer message,
			SelectionKey serverhandle,String coord) throws Exception {
		selectorLock.lock();
		try {
			demultiplexer.wakeup();
			channel.register(demultiplexer, SelectionKey.OP_CONNECT,
					new RemoteMessage(serverhandle, key, message,coord));

		} finally {
			selectorLock.unlock();
		}
	}

	public static void enQueueHandle(SelectionKey client, SelectionKey server,Integer key) {
		handleQueue.put(client, server);
		keyQueue.put(client, key);
	}

	public static SelectionKey deQueueHandle(SelectionKey client) {
		SelectionKey server = handleQueue.get(client);
		handleQueue.remove(server);
		return server;
	}

	public static Selector getDemultiplexer() {
		return demultiplexer;
	}
	
	public static Map<SelectionKey, Integer> getKeyQueue(){
		return keyQueue;
	}
	
	public static Map<SelectionKey, SelectionKey> getHandleQueue(){
		return handleQueue;
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

	public static void stop() {
		stop = true;
	}

}
