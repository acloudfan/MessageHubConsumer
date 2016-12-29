package com.acloudfan.mhub;

import java.io.PrintStream;

public class Util {

	private static final String JAAS_CONFIG_PROPERTY = "java.security.auth.login.config";
	
	/**
	 * The JAAS config is needed by consumer/producer for auth against Bluemix MHub instance
	 * The jaas.conf file has to exist under the directory where the code is getting executed
	 */
	public static void setJaasProperty(){
		String userdir = System.getProperty("user.dir");
		System.setProperty(JAAS_CONFIG_PROPERTY, userdir +"/"+ "jaas.conf");
	}
	
	/**
	 * This is simply to suppress SLF4J warnings - Ignore it
	 */
	public static void suppressSLF4JWarnings(){
		PrintStream filterOut = new PrintStream(System.err) {
		    public void println(String l) {
		        if (! l.startsWith("SLF4J") )
		            super.println(l);
		    }
		};
		System.setErr(filterOut);
	}
}
