package com.myuplay.CS455.Hadoop;

/**
 * A common console simplification class.
 * @author Tyler
 *
 */
public class Console {

	public static void debug(String...outs){
		for (String out : outs){
			System.out.println("[Debug] " + out);
		}
	}

	public static void log(String...outs){
		for (String out : outs){
			System.out.println("[log] " + out);
		}
	}

	public static void error(String...outs){
		for (String out : outs){
			System.out.println("[ERROR] " + out);
		}
	}

	public static void warn(String...outs){
		for (String out: outs){
			System.out.println("[Warn] " + out);
		}
	}

}
