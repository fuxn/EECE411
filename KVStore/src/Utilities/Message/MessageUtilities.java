package Utilities.Message;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MessageUtilities {

	public static byte[] formateRequestMessage(Integer command, byte[] key,
			byte[] value) {
		List<Byte> message = new ArrayList<Byte>();
		message.add(command.byteValue());

		if (key != null) {
			for (int i = 0; i < key.length; i++) {
				message.add(key[i]);
			}
		}

		if (value != null) {
			for (int i = 0; i < value.length; i++) {
				message.add(value[i]);
			}
		}

		byte[] request = new byte[message.size()];
		for (int i = 0; i < message.size(); i++) {
			request[i] = (Byte) message.get(i);
		}
		return request;
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
							&& (System.currentTimeMillis() < endTimeMillis)) {
						if ((bytesRcvd = in.read(reply, totalBytesRcvd,
								reply.length - totalBytesRcvd)) == -1)
							throw new SocketException(
									"connection close prematurely.");
						totalBytesRcvd += bytesRcvd;
					}
					System.out.println("reply : " + Arrays.toString(reply));
					return MessageUtilities.formateReplyMessage(errorCode,
							new String(reply));
				} else
					break;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return MessageUtilities.formateReplyMessage(errorCode, null);
	}

	public static String checkRequestKey(int command, InputStream in) {
		try {
			System.out.println("command : " + command);
			if (MessageUtilities.isCheckRequestKey(command)) {
				System.out.println("Checking request key.. ");
				byte[] key = new byte[32];
				int bytesRcvd;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < key.length) {
					if ((bytesRcvd = in.read(key, totalBytesRcvd, key.length
							- totalBytesRcvd)) == -1)
						throw new SocketException(
								"connection close prematurely.");

					totalBytesRcvd += bytesRcvd;
				}
				return new String(key);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static String checkRequestValue(int command, InputStream in) {
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
				return new String(value);
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
		return (command == 1 || command == 21);
	}

	public static boolean isCheckRequestKey(int command) {
		return (command != 4);
	}

	public static byte[] standarizeMessage(byte[] cmd, int size) {
		List<Byte> message = new ArrayList<Byte>();
		if (cmd.length != size) {
			byte[] temp = new byte[size - cmd.length];
			for (int i = 0; i < temp.length; i++) {
				message.add(temp[i]);
			}
		}

		for (int i = 0; i < cmd.length; i++) {
			message.add(cmd[i]);
		}
		byte[] standarizedMessage = new byte[message.size()];
		for (int i = 0; i < message.size(); i++) {
			standarizedMessage[i] = (Byte) message.get(i);
		}
		return standarizedMessage;
	}
}