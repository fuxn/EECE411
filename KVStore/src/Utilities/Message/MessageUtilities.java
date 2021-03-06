package Utilities.Message;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import Utilities.CommandEnum;

public class MessageUtilities {

	public static byte[] formateRequestMessage(Integer command, byte[] key,
			byte[] value) {
		int messageLength = 1 + (key != null ? key.length : 0)
				+ (value != null ? value.length : 0);
		byte[] message = new byte[messageLength];
		message[0] = command.byteValue();
		if (key != null)
			System.arraycopy(key, 0, message, 1, key.length);
		else
			key = new byte[0];

		if (value != null)
			System.arraycopy(value, 0, message, key.length + 1, value.length);
		return message;
	}

	public static ByteBuffer requestMessage(Integer command, byte[] key,
			byte[] value) {
		int messageLength = 1 + (key != null ? key.length : 0)
				+ (value != null ? value.length : 0);

		byte[] message = new byte[messageLength];
		message[0] = command.byteValue();
		if (key != null)
			System.arraycopy(key, 0, message, 1, key.length);
		else
			key = new byte[0];

		if (value != null)
			System.arraycopy(value, 0, message, key.length + 1, value.length);
		return ByteBuffer.wrap(message);
	}

	public static byte[] formateReplyMessage(Integer errorCode, byte[] value,
			Integer version) {
		int messageLength = 1 + (value != null ? (value.length + 1) : 0);
		byte[] message = new byte[messageLength];
		message[0] = errorCode.byteValue();

		if (value != null) {
			System.arraycopy(value, 0, message, 1, value.length);
			message[message.length - 1] = version.byteValue();
		}
		return message;
	}

	public static byte[] formateReplyMessage(Integer errorCode, byte[] value) {
		int messageLength = 1 + (value != null ? value.length : 0);
		byte[] message = new byte[messageLength];
		message[0] = errorCode.byteValue();

		if (value != null)
			System.arraycopy(value, 0, message, 1, value.length);

		return message;
	}

	public static byte[] formateReplyMessage(Integer errorCode) {
		byte[] m = new byte[1];
		m[0] = errorCode.byteValue();

		return m;
	}

	public static byte[] checkRequestKey(int command, InputStream in) {
		byte[] key = new byte[32];
		try {
			if (MessageUtilities.isCheckRequestKey(command)) {

				int bytesRcvd;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < key.length) {
					if ((bytesRcvd = in.read(key, totalBytesRcvd, key.length
							- totalBytesRcvd)) == -1)
						throw new SocketException(
								"connection close prematurely.");

					totalBytesRcvd += bytesRcvd;
				}
				return key;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return key;

	}

	public static void checkRequestKey(int command,
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
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void checkRequestValue(int command,
			SocketChannel socketChannel, ByteBuffer buffer) {
		try {
			if (MessageUtilities.isCheckRequestValue(command)) {
				int bytesRcvd = 0;
				int totalBytesRcvd = 0;
				while (totalBytesRcvd < buffer.limit()) {
					if ((bytesRcvd = socketChannel.read(buffer)) == -1)
						throw new SocketException(
								"connection close prematurely.");
					totalBytesRcvd += bytesRcvd;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static byte[] checkRequestValue(int command, InputStream in) {
		byte[] value = new byte[1024];
		try {
			if (MessageUtilities.isCheckRequestValue(command)) {
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
		return value;
	}

	public static boolean isGetCommand(int command) {
		return CommandEnum.commandsWithReplyValue.contains(command);
	}
	
	public static boolean isCheckRequestValue(int command) {
		return CommandEnum.commandsWithRequestValue.contains(command);
	}

	public static boolean isCheckRequestKey(int command) {
		return CommandEnum.commandsWithRequestKey.contains(command);
	}

	public static byte[] standarizeMessage(byte[] cmd, int size) {
		if (cmd.length != size) {
			byte[] message = new byte[size];
			System.arraycopy(cmd, 0, message, size - cmd.length, cmd.length);
			return message;
		} else
			return cmd;
	}

	public static byte[] intToByteArray(Integer key, int size) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(size);
		byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
		byteBuffer.putInt(key);
		return byteBuffer.array();
	}

	public static int byteArrayToInt(byte[] key) {
		final ByteBuffer bb = ByteBuffer.wrap(key);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		return bb.getInt();
	}

	/*
	 * public static void checkReplyValue(SocketChannel socketChannel, int
	 * command, ByteBuffer buffer) { if
	 * (MessageUtilities.isCheckReplyValue(command)) { try { int bytesRcvd; int
	 * totalBytesRcvd = 0; while (totalBytesRcvd < buffer.limit()) { if
	 * ((bytesRcvd = socketChannel.read(buffer)) == -1) throw new
	 * SocketException( "connection close prematurely.");
	 * 
	 * totalBytesRcvd += bytesRcvd; }
	 * 
	 * } catch (IOException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } } }
	 */

	/*
	 * public static ByteBuffer handleFailureMessage(Integer command, String
	 * key, String value) { List<Byte> message = new ArrayList<Byte>();
	 * message.add(command.byteValue());
	 * 
	 * if (key != null) { byte[] keyBuffer = key.getBytes(); keyBuffer =
	 * MessageUtilities.standarizeMessage(keyBuffer, 32); for (int i = 0; i <
	 * keyBuffer.length; i++) { message.add(keyBuffer[i]); } }
	 * 
	 * if (value != null) { byte[] valueBuffer = value.getBytes(); for (int i =
	 * 0; i < valueBuffer.length; i++) { message.add(valueBuffer[i]); } }
	 * 
	 * byte[] request = new byte[message.size()]; for (int i = 0; i <
	 * message.size(); i++) { request[i] = (Byte) message.get(i); } return
	 * ByteBuffer.wrap(request);
	 * 
	 * }
	 */
	/*
	 * public static byte[] checkReplyValue(int command, InputStream in) { int
	 * errorCode = -2; try { errorCode = in.read(); if (errorCode == 0 &&
	 * MessageUtilities.isCheckReplyValue(command)) { byte[] reply = new
	 * byte[1024]; int bytesRcvd; int totalBytesRcvd = 0; while ((totalBytesRcvd
	 * < reply.length)) { if ((bytesRcvd = in.read(reply, totalBytesRcvd,
	 * reply.length - totalBytesRcvd)) == -1) throw new SocketException(
	 * "connection close prematurely."); totalBytesRcvd += bytesRcvd; } return
	 * MessageUtilities.formateReplyMessage(errorCode, reply); } } catch
	 * (IOException e) { // TODO Auto-generated catch block e.printStackTrace();
	 * } return MessageUtilities.formateReplyMessage(errorCode, null); }
	 */
}
