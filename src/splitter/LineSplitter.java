package splitter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class LineSplitter extends Splitter {
	BufferedReader reader = null;
	BufferedWriter writer = null;
	
	public LineSplitter() {
	}

	@Override
	protected void split(String input, String output) {
		int num_lines = -1;
		try {
			num_lines = get_num_lines(input);
		} catch (IOException e) {
			e.printStackTrace();
		}

		int lines_per_piece = (int) Math.ceil((double) num_lines
				/ (double) this.conf.num_maps);
		String line;
		try {
			reader = new BufferedReader(new FileReader(input));
		} catch (IOException e) {
			e.printStackTrace();
		}

		int cur_line = 0;
		int piece = 0;

		try {
			while (true) {
				line = reader.readLine();
				if (line == null)
					break;

				if (writer == null) {
					writer = new BufferedWriter(new FileWriter(output + piece,
							true));
					if(piece%10 == 0){
					    System.out.println("Piece " + piece + " " +  num_lines/lines_per_piece );
					}

				}

				writer.write(line + "\n");
				cur_line++;

				if (cur_line == lines_per_piece) {
					piece++;
					cur_line = 0;
					writer.close();
					writer = null;
				}

			}
			if (writer != null) {
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int get_num_lines(String path) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		int lines = 0;
		while (reader.readLine() != null) {
			lines++;
		}
		reader.close();
		return lines;
	}
}
