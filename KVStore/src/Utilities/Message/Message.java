package Utilities.Message;

public class Message {

	private int command;
	private String key;
	private String value;

	public Message(Integer command, byte[] key, byte[] value) {

		this.command = command;
		if (key != null)
			this.key = new String(key);
		if (value != null)
			this.value = new String(value);
	}

	public int getCommand() {
		return this.command;
	}

	public String getKey() {
		return this.key;
	}

	public String getValue() {
		return this.value;
	}
}
