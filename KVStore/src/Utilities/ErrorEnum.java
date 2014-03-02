package Utilities;

public enum ErrorEnum {
	SUCCESS(0), INEXISTENT_KEY(1), OUT_OF_SPACE(2), SYS_OVERLOAD(3), INTERNAL_FAILURE(
			4), UNRECOGNIZED_COMMAND(5), INVALID_KEY(21);

	private int code;

	private ErrorEnum(int c) {
		code = c;
	}

	public int getCode() {
		return code;
	}
}
