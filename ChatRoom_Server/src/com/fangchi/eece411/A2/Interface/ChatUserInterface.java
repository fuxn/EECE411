package com.fangchi.eece411.A2.Interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ChatUserInterface extends Remote {
	public String getUserName() throws RemoteException;
	public boolean broadCastMessage(String message) throws RemoteException;
}
