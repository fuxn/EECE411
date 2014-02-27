package KVStore;

import java.io.IOException;

public class KVStore {

	public static void main(String[] args) throws IOException {
		ProtocolImpl protocol = new ProtocolImpl();
		protocol.initializeServer();
		
		//server wait for incoming requests;
	}
}
