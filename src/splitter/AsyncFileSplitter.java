package splitter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AsyncFileSplitter extends Splitter {
	private String input_path;
	private String output_path;
	private ArrayList<Long> start = new ArrayList<Long>();
	private ArrayList<Long> end = new ArrayList<Long>();
	private AsynchronousFileChannel fileChannel;
	private long chunkSize;
	private long offset = 0;

	// byte[] lf = System.getProperty("line.separator").getBytes();

	public AsyncFileSplitter(long nChunkSize) {
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

	private void start_workers() {
		int cores = Runtime.getRuntime().availableProcessors();
		ExecutorService e = Executors.newFixedThreadPool(cores);
		System.out.println("Executing with " + cores + " threads.");
		long t0 = System.currentTimeMillis();
		try {
			fileChannel = AsynchronousFileChannel.open(Paths.get(input_path));
		} catch (IOException e2) {
			e2.printStackTrace();
		}
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
			System.out.println(split_num + "th task started");
			AsynchronousFileChannel wChannel;
			try {
				ByteBuffer buf = ByteBuffer.allocateDirect((int) ((long) en));
				Future<Integer> x = fileChannel.read(buf, st);
				while (!x.isDone()) {
				}
				System.out.println("Thread" + split_num + " read file");
				buf.flip();
				System.out.println("Thread" + split_num
						+ " starts writing to file");
				wChannel = AsynchronousFileChannel.open(
						Paths.get(output_path + split_num),
						StandardOpenOption.CREATE, StandardOpenOption.WRITE);
				Future<Integer> y = wChannel.write(buf, 0);
				while (!y.isDone()) {
				}
				wChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
