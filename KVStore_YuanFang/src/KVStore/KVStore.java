package KVStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

public class KVStore {

	private static boolean checkForReplyValue = false;

	public static void main(String[] args) {
		try {
			Socket socket = new Socket(args[0].toString(),
					Integer.valueOf(args[1]));
			System.out.println("Connected to Server..");

			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			int commandCode = getCommandCode(args[2].toString());
			if (commandCode == 2)
				checkForReplyValue = true;
			else
				checkForReplyValue = false;

			byte[] v = Message.formateRequestMessage(commandCode,
					args[3].getBytes(), args[4].getBytes());
			out.write(v);
			out.flush();

			int errorCode = in.read();
			System.out.println("error code : " + errorCode);
			if (errorCode == 0 && checkForReplyValue) {
				byte[] reply = new byte[1024];
				int bytesRcvd;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < reply.length) {
					if ((bytesRcvd = in.read(reply, totalBytesRcvd,
							reply.length - totalBytesRcvd)) == -1)
						throw new SocketException(
								"connection close prematurely.");
					totalBytesRcvd += bytesRcvd;
				}
				System.out.println("reply : " + reply.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static int getCommandCode(String cmd) {
		if (cmd.trim().equals("put"))
			return 1;
		else if (cmd.trim().equals("get"))
			return 2;
		else if (cmd.trim().equals("remove"))
			return 3;

		return 0;

	}
}
