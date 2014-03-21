package NIO;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import Utilities.CommandEnum;
import Utilities.Message.Requests;

public class Dispatcher implements Runnable {

	private Map<Integer, EventHandler> registeredHandlers = new ConcurrentHashMap<Integer, EventHandler>();
	private Map<CommandEnum, EventHandler> commandHandlers = new ConcurrentHashMap<CommandEnum, EventHandler>();

	private static Selector demultiplexer;
	private static boolean stop = false;

	public Dispatcher() throws Exception {
		demultiplexer = Selector.open();
	}

	public void registerEventHandler(int eventType, EventHandler eventHandler) {
		registeredHandlers.put(eventType, eventHandler);
	}

	public void registerEventHandler(CommandEnum command,
			EventHandler eventHandler) {
		commandHandlers.put(command, eventHandler);
	}

	// Used to register ServerSocketChannel with the
	// selector to accept incoming client connections
	public void registerChannel(int eventType, SelectableChannel channel)
			throws Exception {
		channel.register(demultiplexer, eventType);
	}

	public static Selector getDemultiplexer() {
		return demultiplexer;
	}

	public void run() {
		try {
			while (!stop) { // Loop indefinitely
				demultiplexer.select();

				Set<SelectionKey> readyHandles = demultiplexer.selectedKeys();
				Iterator<SelectionKey> handleIterator = readyHandles.iterator();

				while (handleIterator.hasNext()) {
					SelectionKey handle = handleIterator.next();

					if (handle.isValid() && handle.isAcceptable()) {
						EventHandler handler = registeredHandlers
								.get(SelectionKey.OP_ACCEPT);
						handler.handleEvent(handle);
						// Note : Here we don't remove this handle from
						// selector since we want to keep listening to
						// new client connections
					}

					if (handle.isValid() && handle.isReadable()) {
						EventHandler handler = registeredHandlers
								.get(SelectionKey.OP_READ);
						handler.handleEvent(handle);
						handleIterator.remove();
					}

					if (handle.isValid() && handle.isWritable()) {

						EventHandler handler = registeredHandlers
								.get(SelectionKey.OP_WRITE);

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
