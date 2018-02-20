package storm.stormLogger;

import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.io.File;

public class StormLogger {
	private Logger Log;
	final private String logFile;
	final private String logName;

	public StormLogger(String logFileAddr, String logName) {
		this.logFile = logFileAddr;
		this.logName = logName;
		initLogger();
	}

	private void initLogger() {
		try {
			FileHandler handler = new FileHandler(logFile, true);
			Log = Logger.getLogger(logName);
			Log.addHandler(handler);
		} catch (java.io.IOException ex1) {
			System.out.println("[Error] Failt to open" + logFile);
		}
	}

	public Logger getLogger(){
		return this.Log;
	}
}