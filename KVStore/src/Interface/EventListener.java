package Interface;

import Exception.InternalKVStoreFailureException;

public interface EventListener {
	public void onConnectionCloseEvent();

	public void onAnnouncedFailure() throws InternalKVStoreFailureException;
}
