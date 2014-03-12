package splitter;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class XMLSplitter extends Splitter {
	private byte[] start_tag; 
	private byte[] end_tag; 
	private InputStream reader = null;
	private DataOutputStream writer = null;
	int num_of_records=0;
	
	public XMLSplitter(String nstart_tag, String nend_tag) {
		super();
		start_tag = nstart_tag.getBytes();
		end_tag = nend_tag.concat("\n").getBytes();
		num_of_records=1000;
	}

	@Override
	protected void split(String input, String output) {
		// TODO Auto-generated method stub
		try {
			reader = new FileInputStream(input);
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
					writer = null;
					if (writer != null) {
						writer.close();
					}
				}
				if (writer == null) {
					writer = new DataOutputStream(new FileOutputStream(output
							+ piece));
				}
				if (readUntilMatch(start_tag, false)) {
					writer.write(start_tag);
				} else {
					break;
				}
				if (readUntilMatch(end_tag, true)) {
					cur_records++;
				}
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

	protected boolean readUntilMatch(byte[] match, boolean withinBlock) {
		int i = 0;
		try {
			while (true) {
				int b = reader.read();
				// end of file:
				if (b == -1) {
					return false;
				}
				// save to buffer:
				if (withinBlock) {
					writer.write(b);
				}

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length) {
						return true;
					}
				} else {
					i = 0;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

}
