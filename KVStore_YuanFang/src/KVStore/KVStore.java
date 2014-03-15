package KVStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class KVStore {

	public static void main(String[] args) {
		try {
			Socket socket = new Socket(args[0].toString(),
					Integer.valueOf(args[1]));
			System.out.println("Connected to Server..");

			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			int commandCode = getCommandCode(args[2].toString());

			byte[] v = Message.formateRequestMessage(commandCode,
					args[3].getBytes(), args[4].getBytes());
			out.write(v);
			out.flush();

			Message.checkReplyValue(commandCode, in);
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
		else if (cmd.trim().equals("announcedFailure"))
			return 4;
		else
			System.out.println("invalid input command");

		return 0;
	}
}
