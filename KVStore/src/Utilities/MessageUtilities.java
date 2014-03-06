package Utilities;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MessageUtilities {

	public static byte[] formateRequestMessage(Integer command, String key,
			String value) {
		StringBuilder message = new StringBuilder();
		message.append(String.valueOf(command));
		message.append(key);
		if (value != null)
			message.append(value);

		return message.toString().getBytes();
	}

	public static byte[] formateReplyMessage(Integer errorCode, String value) {

		List<Byte> message = new ArrayList<Byte>();
		message.add(errorCode.byteValue());
		if (value != null) {
			byte[] valueByte = value.getBytes();
			for (int i = 0; i < valueByte.length; i++) {
				message.add(valueByte[i]);
			}
		}

		byte[] reply = new byte[message.size()];
		for (int i = 0; i < message.size(); i++) {
			reply[i] = message.get(i);
		}
		return reply;
	}

	public static byte[] checkReplyValue(int command, InputStream in) {
		int errorCode = -2;
		long endTimeMillis = System.currentTimeMillis() + 10000;
		while (System.currentTimeMillis() < endTimeMillis) {
			try {
				errorCode = in.read();
				System.out.println("command : " + command);
				System.out.println("error code : " + errorCode);
				if (errorCode == 0
						&& MessageUtilities.isCheckReplyValue(command)) {
					System.out.println("Checking reply value.. ");
					byte[] reply = new byte[1024];
					int bytesRcvd;
					int totalBytesRcvd = 0;
					while ((totalBytesRcvd < reply.length)
							&& (System.currentTimeMillis() > endTimeMillis)) {
						if ((bytesRcvd = in.read(reply, totalBytesRcvd,
								reply.length - totalBytesRcvd)) == -1)
							throw new SocketException(
									"connection close prematurely.");
						totalBytesRcvd += bytesRcvd;
					}
					System.out.println("reply : " + Arrays.toString(reply));
					return MessageUtilities.formateReplyMessage(errorCode,
							new String(reply));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return MessageUtilities.formateReplyMessage(errorCode, null);
	}

	public static byte[] checkRequestValue(int command, InputStream in) {
		try {
			System.out.println("command : " + command);
			if (MessageUtilities.isCheckRequestValue(command)) {
				System.out.println("Checking request value.. ");
				byte[] value = new byte[1024];
				int bytesRcvd = 0;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < value.length) {
					if ((bytesRcvd = in.read(value, totalBytesRcvd,
							value.length - totalBytesRcvd)) == -1)
						throw new SocketException(
								"connection close prematurely.");
					totalBytesRcvd += bytesRcvd;
				}
				return value;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static boolean isCheckReplyValue(int command) {
		return (command == 2);
	}

	public static boolean isCheckRequestValue(int command) {
		return (command == 1);
	}
}
