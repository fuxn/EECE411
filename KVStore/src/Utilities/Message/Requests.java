package Utilities.Message;

import java.nio.ByteBuffer;

import Utilities.CommandEnum;

public class Requests {
	
	private CommandEnum command;
	
	private ByteBuffer reply;
	
	public Requests(CommandEnum command, ByteBuffer reply){
		this.setCommand(command);
		this.setReply(reply);
	}

	public CommandEnum getCommand() {
		return command;
	}

	public void setCommand(CommandEnum command) {
		this.command = command;
	}

	public ByteBuffer getReply() {
		return reply;
	}

	public void setReply(ByteBuffer reply) {
		this.reply = reply;
	}
	
	
	

}
