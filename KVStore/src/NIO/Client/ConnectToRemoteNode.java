package NIO.Client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.List;

import Exception.InternalKVStoreFailureException;
import KVStore.KVStore;
import NIO.Dispatcher;
import Utilities.ChordTopologyService;
import Utilities.CommandEnum;
import Utilities.ConnectionService;
import Utilities.ErrorEnum;
import Utilities.Message.MessageUtilities;

public class ConnectToRemoteNode implements Runnable {

	private String server;
	private byte[] message;
	private SelectionKey serverHandle;
	private boolean waitForReply;
	private Integer cmdForRollingFailure;

	public ConnectToRemoteNode(String server, byte[] message,
			SelectionKey serverHandle, boolean waitForReply,
			Integer cmdForRollingFailure) {
		this.server = server;
		this.message = message;
		this.serverHandle = serverHandle;
		this.waitForReply = waitForReply;
		this.cmdForRollingFailure = cmdForRollingFailure;
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

				Dispatcher.response(serverHandle,
						MessageUtilities.formateReplyMessage(errorCode, reply));
			} else
				Dispatcher.response(serverHandle,
						MessageUtilities.formateReplyMessage(errorCode));

			socket.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			handleRollingFailure();

			ChordTopologyService.connectionFailed(server);
		}
	}

	private void handleRollingFailure() {
		if (this.cmdForRollingFailure == null)
			return;

		if (this.cmdForRollingFailure == CommandEnum.GET_REPLICA.getCode()) {
			List<String> replicas;
			try {
				replicas = ChordTopologyService.getSuccessors(server);
				for (String n : replicas) {
					ConnectionService.connectToSocketReplica(n, serverHandle,
							message, true);
				}
			} catch (InternalKVStoreFailureException e1) {
			}
		} else if (this.cmdForRollingFailure == CommandEnum.PUT_COORD.getCode()) {
			try {
				String coord = ChordTopologyService.getSuccessor(server);
				ConnectionService.connectToSocketRemote(coord, serverHandle,
						message, false, CommandEnum.PUT_COORD.getCode());
			} catch (InternalKVStoreFailureException e) {
			}
		}
	}

}
