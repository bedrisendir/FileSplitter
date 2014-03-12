package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.Properties;

public class Config {
	final public static String conf_marla_jar_path = "build.jar.marla";
	final public static String conf_user_jar_path = "build.jar.user";

	final public static String conf_input_path = "run.input";
	final public static String conf_misc_path = "run.misc";
	final public static String conf_output_path = "run.output";
	final public static String conf_host_filename = "run.hosts";
	final public static String conf_java_path = "run.java";

	final public static String conf_num_hosts = "run.num_hosts";
	final public static String conf_num_maps = "run.num_maps";
	final public static String conf_num_reduces = "run.num_reduces";
	final public static String conf_num_phases = "run.num_phases";
	final public static String conf_heapsize = "run.heapsize";

	final public static String conf_user_mapper = "user.mapper";
	final public static String conf_user_reducer = "user.reducer";

	private static Logger log = Logger.getLogger("Config");

	// Public for now, but these should be changed to private later for
	// encapsulation
	public int num_maps, num_reduces, num_phases, num_hosts;
	public String host_filename, input_path, output_path, misc_path, java_path;
	public String marla_jar_path, user_jar_path;
	public String heapsize;
	public String user_mapper, user_reducer;
	public String[] host_ips;

	public Config() {
		log.info("Loading config...");

		// Load the properties
		Properties file = new Properties();
		try {
			file.load(new FileInputStream("conf/master.properties"));
		} catch (IOException ex) {
			log.severe("conf/master.properties file could not be read. Exiting.");
			//System.exit(1);
		}

		// Read the properties
		num_maps = Integer.parseInt(file.getProperty(conf_num_maps));
		num_reduces = Integer.parseInt(file.getProperty(conf_num_reduces));
		num_phases = Integer.parseInt(file.getProperty(conf_num_phases));
		num_hosts = Integer.parseInt(file.getProperty(conf_num_hosts));

		heapsize = file.getProperty(conf_heapsize);

		user_mapper = file.getProperty(conf_user_mapper);
		user_reducer = file.getProperty(conf_user_reducer);

		marla_jar_path = file.getProperty(conf_marla_jar_path);
		user_jar_path = file.getProperty(conf_user_jar_path);

		host_filename = file.getProperty(conf_host_filename);
		input_path = file.getProperty(conf_input_path);
		output_path = file.getProperty(conf_output_path);
		misc_path = file.getProperty(conf_misc_path);
		java_path = file.getProperty(conf_java_path);

		// Check that the necessary properties are set
		check_property("Number of maps", conf_num_maps, num_maps);
		check_property("Number of reduces", conf_num_reduces, num_reduces);
		check_property("Number of phases", conf_num_phases, num_phases);
		check_property("Number of hosts", conf_num_hosts, num_hosts);

		check_property("User Map class", conf_user_mapper, user_mapper);
		check_property("User Reduce class", conf_user_reducer, user_reducer);

		check_property("MARLA JAR path", conf_marla_jar_path, marla_jar_path);
		check_property("User JAR path", conf_user_jar_path, user_jar_path);

		check_property("Heap size", conf_heapsize, heapsize);

		check_property("Host file", conf_host_filename, host_filename);
		check_property("Input path", conf_input_path, input_path);
		check_property("Output path", conf_output_path, output_path);
		check_property("Misc path", conf_misc_path, misc_path);
		check_property("Java path", conf_java_path, java_path);

		// Check that the properties are set to valid things
		check_file("MARLA JAR path", marla_jar_path);
		check_file("User JAR path", user_jar_path);

		check_file("Host file", host_filename);
		check_file("Input path", input_path);
		check_file("Output path", output_path);
		check_file("Misc path", misc_path);

		if (java_path.equals("ENV_JAVA_HOME")) {
			java_path = "${JAVA_HOME}/bin/java";
		} else {
			check_file("Java path", java_path);
		}

		// Read the host IPs
		ArrayList<String> temp_host_ips = new ArrayList<String>();
		BufferedReader temp;
		String line;

		try {
			temp = new BufferedReader(new FileReader(host_filename));

			while (true) {
				line = temp.readLine();
				if (line == null) {
					break;
				}

				line = line.trim();
				if (line.isEmpty() || line.charAt(0) == '#') {
					continue;
				}

				temp_host_ips.add(line);
			}
			temp.close();
		} catch (IOException ex) {
			log.severe("Host file could not be read (" + host_filename
					+ "). Exiting.");
			//System.exit(1);
		}

		if (temp_host_ips.size() <= 0) {
			log.severe("Host file is empty. Exiting.");
			//System.exit(1);
		}

		if (num_hosts == 0) {
			log.info("Setting number of hosts automatically...\n\trun.num_hosts = "
					+ temp_host_ips.size());
			num_hosts = temp_host_ips.size();
		} else if (temp_host_ips.size() != num_hosts) {
			log.warning("Number of hosts found did not match configuration ("
					+ temp_host_ips.size() + " found, " + num_hosts
					+ " expected). Exiting.");
			//System.exit(1);
		}

		host_ips = new String[temp_host_ips.size()];
		host_ips = temp_host_ips.toArray(host_ips);
	}
	
//////////////ALLRETURNS TRUE
	
	// Checks if a property is "set"
	private boolean check_property(String label, String property, int number) {
		if (number < 0) {
			//log.warning(label + " not set (" + property + "). Exiting.");
			//System.exit(1);

			//return false;
		}
		return true;
	}

	private boolean check_property(String label, String property, String test) {
		if (test == null || test.trim().equals("")) {
			//log.warning(label + " not set (" + property + "). Exiting.");
			//System.exit(1);
			//return false;
		}
		return true;
	}

	// Checks if a file can be read
	private boolean check_file(String label, String name) {
		File temp = new File(name);
		if (!temp.exists()) {
			//log.severe(label + " could not be read (" + name + "). Exiting.");
			//System.exit(1);
			//return false;
		}
		return true;
	}
}