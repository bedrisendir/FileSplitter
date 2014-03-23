package splitter;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexSplitter extends Splitter {
	private Pattern pattern;
	private int num_of_records;
	private BufferedWriter writer = null;
	private InputStreamReader dis = null;
	private InputStream reader = null;
	private int group_id = -1;

	/**
	 * @param p Regex Pattern
	 * @param n_num_of_records Number of Records per file
	 */
	public RegexSplitter(Pattern p, int n_num_of_records) {
		super();
		pattern = p;
		num_of_records = n_num_of_records;
	}

	/**
	 * @param p Regex Pattern
	 * @param n_num_of_records Number of Records per file
	 * @param n_groupid Group ID inside of the captured pattern
	 */
	public RegexSplitter(Pattern p,int n_num_of_records,int n_groupid) {
		super();
		pattern = p;
		num_of_records = n_num_of_records;
		if (n_groupid < 0) {
			// log.severe("Negative group id");
			System.exit(1);
		}
		group_id = n_groupid;
	}

	protected void split(String input, String output) {
		try {
			reader = new FileInputStream(input);
			dis = new InputStreamReader(reader);
		} catch (IOException e) {
			e.printStackTrace();
		}

		int cur_records = 0;
		int piece = 0;

		try {
			while (true) {
				if (cur_records == num_of_records) {
					cur_records = 0;
					piece++;
					writer.close();
					writer = null;
				}
				if (writer == null) {
					writer = new BufferedWriter(new FileWriter(output + piece,
							true));
				}
				if (readUntilMatch(pattern)) {
					cur_records++;
				} else {
					break;
				}
			}
			if (writer != null) {
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private boolean readUntilMatch(Pattern p) {
		StringBuilder item = new StringBuilder("");
		Matcher m = p.matcher(item);
		while (!m.find()) {
			try {
				int c = dis.read();
				if (c == -1) {
					return false;
				}
				item.append((char) c);
				m = p.matcher(item);
			} catch (IOException e) {

				e.printStackTrace();
			}
		}
		try {
			if (group_id != -1) {
				writer.write((m.group(group_id)) + "\n");
			} else {
				writer.write(m.group()+"\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}
