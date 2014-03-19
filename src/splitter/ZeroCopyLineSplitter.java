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

public class ZeroCopyLineSplitter {
	ArrayList<Long> start = new ArrayList<Long>();
	ArrayList<Long> end = new ArrayList<Long>();
	long chunkSize;
	long offset = 0;

	public ZeroCopyLineSplitter(long nChunkSize) {
		chunkSize = nChunkSize;
	}

	public void map() {
		File file = new File(
				"/home/bsendir1/workspacemarla/materials_dbv2-04052013.json");

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
			System.out
					.println("ITEMS " + start.size() + " " + end.size() + " ");
		} catch (IOException e) {
			// TODO Auto-generated catch block
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

	public void split() throws IOException {
		long t0 = System.currentTimeMillis();
		int cores = Runtime.getRuntime().availableProcessors();
		ExecutorService e = Executors.newFixedThreadPool(2);
		System.out.println("Executing with " + cores + " threads.");
	

		for (int i = 0; i < start.size(); i++) {
			e.execute(new SplitWorker(start.get(i), end.get(i), i));
		}
		e.shutdown();
		try {
			e.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		long t1 = System.currentTimeMillis();
		System.out.println("Splitting: " + (t1 - t0) + "ms");
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
				destination = new FileOutputStream(
						"/home/bsendir1/workspacemarla/FileSplitter_new/splits/temp"
								+ split_num).getChannel();
				long p1 = System.currentTimeMillis();
				source = new FileInputStream(
						"/home/bsendir1/workspacemarla/materials_dbv2-04052013.json")
						.getChannel();
				long p2 = System.currentTimeMillis();
				System.out.println("Channel costs :" + (p2-p1));
				source.position(st);
				long p3 = System.currentTimeMillis();
				System.out.println("Seeking costs :" + (p3-p2));
				destination.transferFrom(source,0,en);
				long p4 = System.currentTimeMillis();
				System.out.println("Transfer costs :" + (p4-p3));
				System.out.println("Split "+ split_num + " left source at "+ source.position());
				destination.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
