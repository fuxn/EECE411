package NIO_Client;

import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import NIO.EventHandler;
import Utilities.Message.RemoteMessage;

public class ClientDispatcher implements Runnable{
	private Map<Integer, EventHandler> registeredHandlers = new ConcurrentHashMap<Integer, EventHandler>();
	private static Selector demultiplexer;

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

	public static void registerChannel(int eventType, SelectableChannel channel,
			Selector serverSelector, SelectionKey serverHandle,
			ByteBuffer message) throws Exception {
		SelectionKey key = channel.register(demultiplexer, eventType);
		key.attach(new RemoteMessage(serverSelector, key, message));

	}

	public Selector getDemultiplexer() {
		return demultiplexer;
	}

	public void run() {
		try {
			while (true) { // Loop indefinitely
				demultiplexer.select();

				Set<SelectionKey> readyHandles = demultiplexer.selectedKeys();
				System.out
						.println("ready handles size: " + readyHandles.size());
				Iterator<SelectionKey> handleIterator = readyHandles.iterator();

				while (handleIterator.hasNext()) {
					SelectionKey handle = handleIterator.next();
					if (handle.isReadable()) {
						System.out.println("reading");
						EventHandler handler = (EventHandler) this.registeredHandlers
								.get(SelectionKey.OP_READ);
						handler.handleEvent(handle);
						handleIterator.remove();
					}

					if (handle.isWritable()) {
						System.out.println("writing");
						EventHandler handler = (EventHandler) this.registeredHandlers
								.get(SelectionKey.OP_WRITE);
						handler.handleEvent(handle);
						handleIterator.remove();
					}

					if (handle.isConnectable()) {
						System.out.println("connecting");
						EventHandler handler = (EventHandler) this.registeredHandlers
								.get(SelectionKey.OP_CONNECT);
						handler.handleEvent(handle);
						handleIterator.remove();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
}
