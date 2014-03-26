package Utilities;

import java.util.ArrayList;
import java.util.List;

public enum CommandEnum {
	PUT(1), GET(2), DELETE(3), ANNOUNCE_FAILURE(4), HANDLE_ANNOUNCED_FAILURE(21), ANNOUNCE_LEAVING(
			22), ANNOUNCE_JOINING(23), DATA_SENT(24);

	private int code;

	private CommandEnum(int c) {
		code = c;
	}

	public int getCode() {
		return code;
	}

	public final static List<Integer> commandsWithRequestKey = new ArrayList<Integer>();
	static {
		commandsWithRequestKey.add(PUT.getCode());
		commandsWithRequestKey.add(GET.getCode());
		commandsWithRequestKey.add(DELETE.getCode());
		commandsWithRequestKey.add(HANDLE_ANNOUNCED_FAILURE.getCode());
		commandsWithRequestKey.add(ANNOUNCE_JOINING.getCode());
		commandsWithRequestKey.add(ANNOUNCE_LEAVING.getCode());
	}

	public final static List<Integer> commandsWithRequestValue = new ArrayList<Integer>();
	static {
		commandsWithRequestValue.add(PUT.getCode());
		commandsWithRequestValue.add(HANDLE_ANNOUNCED_FAILURE.getCode());
		
	}

	public final static List<Integer> commandsWithReply = new ArrayList<Integer>();
	static {
		commandsWithReply.add(PUT.getCode());
		commandsWithReply.add(GET.getCode());
		commandsWithReply.add(DELETE.getCode());
	}

	public final static List<Integer> commandsWithReplyValue = new ArrayList<Integer>();

	static {
		commandsWithReplyValue.add(GET.getCode());
	}
}
