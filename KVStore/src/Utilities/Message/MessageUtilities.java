package Utilities.Message;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import Utilities.CommandEnum;

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

	public static ByteBuffer requestMessage(Integer command, String key,
			String value) {
		List<Byte> message = new ArrayList<Byte>();
		message.add(command.byteValue());

		if (key != null) {
			byte[] keyBuffer = key.getBytes();
			for (int i = 0; i < keyBuffer.length; i++) {
				message.add(keyBuffer[i]);
			}
		}

		if (value != null) {
			byte[] valueBuffer = value.getBytes();
			for (int i = 0; i < valueBuffer.length; i++) {
				message.add(valueBuffer[i]);
			}
		}

		byte[] request = new byte[message.size()];
		for (int i = 0; i < message.size(); i++) {
			request[i] = (Byte) message.get(i);
		}
		return ByteBuffer.wrap(request);
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

	public static String checkReplyValue(SocketChannel socketChannel,
			int command, ByteBuffer buffer) {
		if (MessageUtilities.isCheckReplyValue(command)) {
			long endTimeMillis = System.currentTimeMillis() + 10000;
			try {
				int bytesRcvd;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < buffer.limit()
						&& System.currentTimeMillis() < endTimeMillis) {
					if ((bytesRcvd = socketChannel.read(buffer)) == -1)
						throw new SocketException(
								"connection close prematurely.");

					totalBytesRcvd += bytesRcvd;
				}

				buffer.flip();
				byte[] value = new byte[buffer.limit()];
				buffer.get(value);

				return new String(value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return new String();
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

	public static String checkRequestKey(int command,
			SocketChannel socketChannel, ByteBuffer buffer) {
		try {
			if (MessageUtilities.isCheckRequestKey(command)) {

				int bytesRcvd;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < buffer.limit()) {
					if ((bytesRcvd = socketChannel.read(buffer)) == -1)
						throw new SocketException(
								"connection close prematurely.");

					totalBytesRcvd += bytesRcvd;
				}

				buffer.flip();
				byte[] key = new byte[buffer.limit()];
				buffer.get(key);

				return new String(key);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new String();
	}

	public static String checkRequestValue(int command,
			SocketChannel socketChannel, ByteBuffer buffer) {
		try {
			if (MessageUtilities.isCheckRequestValue(command)) {
				System.out.println("Checking request value.. ");
				int bytesRcvd = 0;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < buffer.limit()) {
					if ((bytesRcvd = socketChannel.read(buffer)) == -1)
						throw new SocketException(
								"connection close prematurely.");
					totalBytesRcvd += bytesRcvd;
				}

				buffer.flip();
				byte[] value = new byte[buffer.limit()];
				buffer.get(value);
				return new String(value);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new String();
	}

	public static String checkRequestValue(int command, InputStream in) {
		try {
			if (MessageUtilities.isCheckRequestValue(command)) {
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
		return new String();
	}

	public static boolean isCheckReplyValue(int command) {
		return (command == 2);
	}

	public static boolean isCheckReply(int command) {
		return (command == CommandEnum.PUT.getCode()
				|| command == CommandEnum.GET.getCode() || command == CommandEnum.DELETE
					.getCode());
	}

	public static boolean isCheckRequestValue(int command) {
		return (command != CommandEnum.DELETE.getCode() && command != CommandEnum.GET
				.getCode());
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

	public static ByteBuffer handleFailureMessage(Integer command, String key,
			String value) {
		List<Byte> message = new ArrayList<Byte>();
		message.add(command.byteValue());

		if (key != null) {
			byte[] keyBuffer = key.getBytes();
			keyBuffer = MessageUtilities.standarizeMessage(keyBuffer, 32);
			for (int i = 0; i < keyBuffer.length; i++) {
				message.add(keyBuffer[i]);
			}
		}

		if (value != null) {
			byte[] valueBuffer = value.getBytes();
			for (int i = 0; i < valueBuffer.length; i++) {
				message.add(valueBuffer[i]);
			}
		}

		byte[] request = new byte[message.size()];
		for (int i = 0; i < message.size(); i++) {
			request[i] = (Byte) message.get(i);
		}
		return ByteBuffer.wrap(request);

	}
}
