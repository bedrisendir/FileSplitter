package splitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ZeroCopyLineSplitter extends Splitter{
	private ArrayList<Long> start = new ArrayList<Long>();
	private ArrayList<Long> end = new ArrayList<Long>();
	private long chunkSize;
	private long offset = 0;
	private String input_path;
	private String output_path;
	
	public ZeroCopyLineSplitter(long nChunkSize) {
		chunkSize = nChunkSize;
	}

	private void map() {
		File file = new File(input_path);
		try {
			RandomAccessFile raf;
			raf = new RandomAccessFile(file, "rw");
			System.out.println(file.length());
			raf.setLength(file.length());
			long t0 = System.currentTimeMillis();
			System.out.println(raf.length());

			while (offset < raf.length()) {
				if ((offset + chunkSize) > raf.length()) {
					chunkSize = raf.length() - offset;
				}
				long diff = findNextLine(raf);
				start.add(offset);
				end.add(chunkSize + diff);
				offset = offset + chunkSize + diff;
			}
			raf.close();
			long t1 = System.currentTimeMillis();
			System.out.println("Mapping Took: " + (t1 - t0) + "ms");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private long findNextLine(RandomAccessFile raf) throws IOException {
		raf.seek(offset + chunkSize);
		long old = raf.getFilePointer();
		System.out.println("Pre:" + raf.getFilePointer());
		raf.readLine();
		System.out.println("Post:" + raf.getFilePointer());
		long newp = raf.getFilePointer();
		return newp - old;
	}

	private void start_workers(){
		long t0 = System.currentTimeMillis();
		int cores = Runtime.getRuntime().availableProcessors();
		ExecutorService e = Executors.newFixedThreadPool(cores);
		System.out.println("Executing with " + cores + " threads.");
		for (int i = 0; i < start.size(); i++) {
			e.execute(new SplitWorker(start.get(i), end.get(i), i));
		}
		e.shutdown();
		try {
			e.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		long t1 = System.currentTimeMillis();
		System.out.println("Splitting: " + (t1 - t0) + "ms");
	}
	
	@Override
	protected void split(String input, String output) {
		input_path = input;
		output_path = output;
		map();
		start_workers();
	}
	public class SplitWorker implements Runnable {
		int split_num;
		Long st;
		Long en;

		public SplitWorker(Long nstart, Long nend, int i) {
			st = nstart;
			en = nend;
			split_num = i;
		}
		
		public void run() {
			try {
				FileChannel source = null;
				FileChannel destination = null;
				destination = new FileOutputStream(output_path + split_num).getChannel();
				//long p1 = System.currentTimeMillis();
				source = new FileInputStream(input_path)
						.getChannel();
				//long p2 = System.currentTimeMillis();
				//System.out.println("Channel costs :" + (p2-p1));
				source.position(st);
				//long p3 = System.currentTimeMillis();
				//System.out.println("Seeking costs :" + (p3-p2));
				destination.transferFrom(source,0,en);
				//long p4 = System.currentTimeMillis();
				//System.out.println("Transfer costs :" + (p4-p3));
				destination.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
