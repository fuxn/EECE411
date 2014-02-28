package Exception;

public class InvalidKeyException extends Exception {

	private String errorMessage = new String();

	public InvalidKeyException(String msg) {
		this.errorMessage = msg;
	}

	public String getErrorMesssage() {
		return this.errorMessage;
	}
}
