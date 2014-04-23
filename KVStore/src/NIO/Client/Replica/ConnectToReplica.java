package NIO.Client.Replica;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.SelectionKey;

import KVStore.KVStore;
import NIO.Dispatcher;
import Utilities.ChordTopologyService;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class ConnectToReplica implements Runnable {
	private String server;
	private byte[] message;
	private SelectionKey serverHandle;
	private boolean waitForReply;

	public ConnectToReplica(String server, byte[] message,
			SelectionKey serverHandle, boolean waitForReply) {
		this.server = server;
		this.message = message;
		this.serverHandle = serverHandle;
		this.waitForReply = waitForReply;
	}

	@Override
	public void run() {
		try {
			Socket socket = new Socket(server, KVStore.NIO_REPLICA_PORT);
			OutputStream out = socket.getOutputStream();
			out.write(message);
			out.flush();

			InputStream in = socket.getInputStream();

			int errorCode = in.read();

			if ((errorCode == ErrorEnum.SUCCESS.getCode()) && waitForReply) {
				byte[] reply = new byte[1024];
				int bytesRcvd;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < reply.length) {
					if ((bytesRcvd = in.read(reply, totalBytesRcvd,
							reply.length - totalBytesRcvd)) == -1) {
						throw new SocketException(
								"connection close prematurely.");
					}

					totalBytesRcvd += bytesRcvd;
				}

				System.out.println("get from replica " + new String(reply));
				Dispatcher.response(serverHandle,
						MessageUtilities.formateReplyMessage(errorCode, reply));
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			ChordTopologyService.connectionFailed(server);
		}

	}
}
