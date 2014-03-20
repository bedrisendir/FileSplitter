package splitter;

import java.io.File;
import java.util.logging.Logger;

import util.Config;

public abstract class Splitter {
	private static Logger log = Logger.getLogger("Splitter");
	protected Config conf;
	private String[] files = null;
	private String path = null;

	public void make_splits() {

		log.info("Initializing splits...");
		this.conf = new Config();
		path = conf.input_path;

		log.info("Retrieving input files...");
		files = get_files();

		log.info("Creating initial splits...");
		for (int i = 0; i < this.files.length; i++) {
			String cur_file = this.files[i];
			split(cur_file,this.conf.misc_path + "/input.task_"+i+"_");
		}

		log.info("Split " + this.files.length + " files across "
				+ this.conf.num_hosts + " machines...");

	}

	protected String[] get_files() {
		File file = new File(path);
		String[] return_array = null;

		// single file
		if (file.isFile()) {
			return_array = new String[1];
			return_array[0] = path;

		// directory
		} else if (file.isDirectory()) {
			return_array = file.list();

			for (int i = 0; i < return_array.length; i++) {
				return_array[i] = path + "/" + return_array[i];
			}

		// doesn't exist
		} else {
			log.severe("Input file is neither a directory or a file. Exiting.");
			System.exit(1);
		}
		return return_array;
	}
	
	abstract protected void split(String input, String output);
}