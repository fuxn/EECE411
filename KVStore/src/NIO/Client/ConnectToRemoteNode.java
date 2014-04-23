package NIO.Client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import KVStore.KVStore;
import NIO.Dispatcher;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class ConnectToRemoteNode implements Runnable {

	private String server;
	private byte[] message;
	private SelectionKey serverHandle;
	private boolean waitForReply;

	public ConnectToRemoteNode(String server, byte[] message,
			SelectionKey serverHandle, boolean waitForReply) {
		this.server = server;
		this.message = message;
		this.serverHandle = serverHandle;
		this.waitForReply = waitForReply;
	}

	@Override
	public void run() {
		try {
			Socket socket = new Socket(server, KVStore.NIO_SERVER_PORT);
			OutputStream out = socket.getOutputStream();
			out.write(message);
			out.flush();

			InputStream in = socket.getInputStream();

			int errorCode = in.read();
			System.out.println("connectToRemote " + server + "command :"
					+ message[0] + " error code :" + errorCode);

			if (waitForReply && (errorCode == ErrorEnum.SUCCESS.getCode())) {
				byte[] reply = new byte[1024];

				try {
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

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Dispatcher.response(serverHandle,
						MessageUtilities.formateReplyMessage(errorCode, reply));
			} else
				Dispatcher.response(serverHandle,
						MessageUtilities.formateReplyMessage(errorCode));

			socket.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
