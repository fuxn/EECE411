package com.fangchi.eece411.A2.Interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.fangchi.eece411.A2.Utilities.Message;


public interface ChatRoomInterface extends Remote {
	public boolean register(ChatUserInterface client) throws RemoteException;
	public boolean unregister(ChatUserInterface client) throws RemoteException;
    public boolean postMessage(Message message) throws RemoteException;
}
