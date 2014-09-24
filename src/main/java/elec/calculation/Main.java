package elec.calculation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Main {
	private static String LINE_SEPARATOR = System.getProperty("line.separator");
	private static final String CONFIG_FILE_NAME = "elec-config.properties";
	private static String DIR_PATH = "configure"
			+ System.getProperty("file.separator");
	private static Logger logger = Logger.getLogger(Main.class);
	private static int PORT_TO_SERVE;
	public static boolean IS_TO_SAVE_DETAIL;

	static {
		init();
	}

	public static void main(String[] args) {
		AgentServer agentServer = new AgentServer(PORT_TO_SERVE);
		agentServer.start();
	}

	public static void init() {
		guardConfigureExists();
		initProperties();
	}

	private static void guardConfigureExists() {
		File dirFile = new File(DIR_PATH);
		if (!dirFile.exists()) {
			dirFile.mkdirs();
		}
		File configFile = new File(DIR_PATH + CONFIG_FILE_NAME);
		if (!configFile.exists()) {
			createDefaultConfig(configFile);
		}
	}

	private static void createDefaultConfig(File configFile) {
		try {
			if (configFile.createNewFile()) {
				FileOutputStream outputStream = new FileOutputStream(configFile);
				String content = "isToSaveDetailResult=false" + LINE_SEPARATOR
						+ "servePort=9999";
				outputStream.write(content.getBytes());
				outputStream.flush();
				outputStream.close();
			}
		} catch (Exception e) {
		}
	}

	private static void initProperties() {
		try {
			FileInputStream inputStream = new FileInputStream(new File(DIR_PATH
					+ CONFIG_FILE_NAME));
			Properties properties = new Properties();
			properties.load(inputStream);
			PORT_TO_SERVE = Integer.parseInt((String) properties
					.get("servePort"));
		} catch (IOException e) {
			logger.error("There is an error when getPortToServe!");
		}
	}

	
}
