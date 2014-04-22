package NIO.Client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import KVStore.KVStore;
import NIO.Dispatcher;
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

			if (waitForReply) {
				byte[] reply = new byte[1024];
				in.read(reply);
				Dispatcher.response(serverHandle,
						MessageUtilities.formateReplyMessage(errorCode, reply));
			} else
				Dispatcher.response(serverHandle,
						MessageUtilities.formateReplyMessage(errorCode));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
