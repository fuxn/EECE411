package KVStore;

import java.util.ArrayList;
import java.util.List;

public class Message {

	public static byte[] formateRequestMessage(Integer command, byte[] key,
			byte[] value) {
		List<Byte> message = new ArrayList<Byte>();
		message.add(command.byteValue());

		message.addAll(standarizeMessage(key, 32));
		if (value != null)
			message.addAll(standarizeMessage(value, 1024));

		byte[] request = new byte[message.size()];
		for (int i = 0; i < message.size(); i++) {
			request[i] = (Byte) message.get(i);
		}
		return request;
	}

	public static byte[] formateReplyMessage(Integer errorCode, byte[] value) {
		System.out.println(errorCode);
		List<Byte> message = new ArrayList<Byte>();
		message.add(errorCode.byteValue());
		if (value != null && value.length > 0) {
			for (int i = 0; i < value.length; i++) {
				message.add(value[i]);
			}
		}

		byte[] reply = new byte[message.size()];
		for (int i = 0; i < message.size(); i++) {
			reply[i] = message.get(i);
		}
		return reply;
	}

	private static List<Byte> standarizeMessage(byte[] cmd, int size) {
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

		System.out.println("message size" + message.size());
		return message;
	}
}
