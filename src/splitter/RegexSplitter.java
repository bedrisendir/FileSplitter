package splitter;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexSplitter extends Splitter {
	Pattern pattern;
	Matcher m;
	int num_of_matches;
	private BufferedWriter writer = null;
	private InputStreamReader dis = null;
	private InputStream reader = null;

	public RegexSplitter(Pattern p) {
		super();
		pattern = p;
		num_of_matches = 10;
	}

	protected void split(String input, String output) {
		// TODO Auto-generated method stub
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
				if (cur_records == num_of_matches) {
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
			// System.out.println("FALSE" + item);
			try {
				int c = dis.read();
				if(c==-1){
					return false;
				}
				item.append((char) c);
				m = p.matcher(item);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			// System.out.println("TRUE" + m.group());
			writer.write((m.group(1))+"\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}
