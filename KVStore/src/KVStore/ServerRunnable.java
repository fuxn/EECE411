package KVStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;

import Exception.InexistentKeyException;
import Exception.InternalKVStoreFailureException;
import Exception.InvalidKeyException;
import Exception.OutOfSpaceException;
import Exception.SystemOverloadException;
import Exception.UnrecognizedCommandException;
import Interface.ConsistentHashInterface;
import Interface.EventListener;
import Utilities.ErrorEnum;
import Utilities.Message.Message;
import Utilities.Message.MessageUtilities;

public class ServerRunnable implements Runnable {
	private Socket server;
	private Message message;
	private ConsistentHashInterface cHash;
	private EventListener listener;

	static volatile boolean keepRunning = true;

	public ServerRunnable(Socket server, ConsistentHashInterface cHash2,
			EventListener listener) {
		this.server = server;
		this.cHash = cHash2;
		this.listener = listener;
	}

	@Override
	public void run() {

		try {

			this.message = this.onReceiveMessage(this.server);

			System.out.println("new command executed: ");

			OutputStream writer = this.server.getOutputStream();

			try {

				byte[] results = this.exec(this.message.getCommand(),
						this.message.getKey(), this.message.getValue());

				if (results != null) {
					System.out.println("result " + Arrays.toString(results));
					writer.write(results);
					writer.flush();
				}

			} catch (InexistentKeyException ex) {
				writer.write(ErrorEnum.INEXISTENT_KEY.getCode());
				writer.flush();
			} catch (UnrecognizedCommandException uc) {
				writer.write(ErrorEnum.UNRECOGNIZED_COMMAND.getCode());
				writer.flush();
			} catch (InternalKVStoreFailureException internalException) {
				writer.write(ErrorEnum.INTERNAL_FAILURE.getCode());
				writer.flush();
			} catch (InvalidKeyException invalideKeyException) {
				writer.write(ErrorEnum.INVALID_KEY.getCode());
				writer.flush();
			} catch (OutOfSpaceException e) {
				writer.write(ErrorEnum.OUT_OF_SPACE.getCode());
				writer.flush();
			}

			this.connectionClose();

		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (SystemOverloadException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InternalKVStoreFailureException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	private Message onReceiveMessage(Socket client)
			throws SystemOverloadException, InternalKVStoreFailureException {

		try {
			InputStream reader = client.getInputStream();

			int command = reader.read();
			byte[] key = MessageUtilities.checkRequestKey(command, reader);
			byte[] value = MessageUtilities.checkRequestValue(command, reader);

			return new Message(client, command, key, value);
		} catch (SocketException e) {
			throw new InternalKVStoreFailureException();
		} catch (IOException e) {
			throw new InternalKVStoreFailureException();
		}

	}

	private byte[] exec(int command, String key, String value)
			throws InexistentKeyException, UnrecognizedCommandException,
			InternalKVStoreFailureException, InvalidKeyException,
			OutOfSpaceException {
		System.out.println("executing command " + command);
		if (command == 1)
			return cHash.put(key, value);
		else if (command == 2)
			return cHash.get(key);
		else if (command == 3)
			return cHash.remove(key);
		else if (command == 4)
			return this.announceFailure();
		else if (command == 21)
			return cHash.handleNeighbourAnnouncedFailure(key, value);
		else
			throw new UnrecognizedCommandException();
	}

	private byte[] announceFailure() throws InternalKVStoreFailureException {
		this.listener.onAnnouncedFailure();
		return MessageUtilities.formateReplyMessage(
				ErrorEnum.SUCCESS.getCode(), null);
	}

	private void connectionClose() throws IOException {
		this.server.close();
		this.listener.onConnectionCloseEvent();
	}

}