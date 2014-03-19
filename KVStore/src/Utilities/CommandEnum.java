package Utilities;

public enum CommandEnum {
	PUT(1), GET(2), DELETE(3), HANDLE_ANNOUNCED_FAILURE(21), ANNOUNCE_LEAVING(
			22), ANNOUNCE_JOINING(23), DATA_SENT(24);

	private int code;

	private CommandEnum(int c) {
		code = c;
	}

	public int getCode() {
		return code;
	}

}
