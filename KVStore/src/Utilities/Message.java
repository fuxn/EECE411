package Utilities;

import java.util.ArrayList;
import java.util.List;

public class Message {

	public static byte[] formateRequestMessage(Integer command, byte[] key,
			byte[] value) {
		List<Byte> message = new ArrayList<Byte>();
		message.add(command.byteValue());

		for (int i = 0; i < key.length; i++) {
			message.add(key[i]);
		}

		if (value != null)
			for (int i = 0; i < value.length; i++) {
				message.add(value[i]);
			}

		byte[] request = new byte[message.size()];
		for (int i = 0; i < message.size(); i++) {
			request[i] = (Byte) message.get(i);
		}
		return request;
	}

	public static byte[] formateReplyMessage(Integer errorCode, byte[] value) {
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
}
