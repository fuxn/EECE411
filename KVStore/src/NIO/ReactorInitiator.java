package NIO;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;

public class ReactorInitiator {
	 
private static final int NIO_SERVER_PORT = 4560;
 
  public void initiateReactiveServer(String localHostName,Collection<String> nodes) throws Exception {
	  System.out.println("Starting NIO server at port : " +
		      NIO_SERVER_PORT);
	  
    ServerSocketChannel server = ServerSocketChannel.open();
    server.socket().bind(new InetSocketAddress(localHostName,NIO_SERVER_PORT));
    server.configureBlocking(false);
 
    Dispatcher dispatcher = new Dispatcher();
    dispatcher.registerChannel(SelectionKey.OP_ACCEPT, server);
 
    dispatcher.registerEventHandler(
      SelectionKey.OP_ACCEPT, new AcceptEventHandler(
      dispatcher.getDemultiplexer()));
 
    dispatcher.registerEventHandler(
      SelectionKey.OP_READ, new ReadEventHandler(
      dispatcher.getDemultiplexer(),nodes));
 
    dispatcher.registerEventHandler(
    SelectionKey.OP_WRITE, new WriteEventHandler());
 
    dispatcher.run(); // Run the dispatcher loop
 
 }
 
}
 

 

 

