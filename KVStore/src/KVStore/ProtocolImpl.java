package KVStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Exception.SystemOverloadException;
import Exception.UnrecognizedCommandException;
import Interface.EventListener;
import Interface.ConsistentHashInterface;
import Utilities.ConsistentHash;
import Utilities.ErrorEnum;
import Utilities.PlanetLabNode;
import Utilities.Message.Message;
import Utilities.Message.MessageQueue;
import Utilities.Message.MessageUtilities;
import Utilities.Thread.ThreadPool;

public class ProtocolImpl implements EventListener {

	private int portNumber = 4560;
	private ConsistentHashInterface cHash;
	private static ThreadPool threadPool;

	private static ServerSocket serverSocket;
	private static int maxConnections = 50;
	private static int maxThreads = 5;
	private static int maxTasks = 40000;

	private int numConnection = 0;

	static volatile boolean keepRunning = true;

	public ProtocolImpl(Collection<PlanetLabNode> nodes) {
		this.cHash = new ConsistentHash(1, nodes);

		threadPool = new ThreadPool(maxThreads, maxTasks);
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run(){
				try {
					threadPool.stop();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});


	}

	public void startServer() {
		try {
			serverSocket = new ServerSocket(portNumber);
			System.out.println("Server waiting for client");
			Socket server;

			while (true) {
				server = serverSocket.accept();
				if (!this.isOverloaded()) {
					ServerRunnable serverRunnable = new ServerRunnable(server,
							this.cHash, this);
					try {
						this.threadPool.execute(serverRunnable);
					} catch (SystemOverloadException e) {
						this.systemOverLoad(server.getOutputStream());
					}
				} else {
					this.systemOverLoad(server.getOutputStream());
					server.close();
				}
			}

		} catch (IOException e) {

		}

	}

	public boolean isOverloaded() {
		return (this.numConnection++ > maxConnections) || (maxConnections == 0);
	}

	@Override
	public void onConnectionCloseEvent() {
		this.numConnection--;
	}

	private void systemOverLoad(OutputStream writer) throws IOException {
		writer.write(ErrorEnum.SYS_OVERLOAD.getCode());
		writer.flush();
	}

	@Override
	public void onAnnouncedFailure() {
		System.exit(0);		
	}

}
