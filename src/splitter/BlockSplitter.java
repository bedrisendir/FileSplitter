package splitter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BlockSplitter {
	ArrayList<MappedByteBuffer> maps = new ArrayList<MappedByteBuffer>();
	long chunkSize;

	public BlockSplitter(long nChunkSize) {
		chunkSize = nChunkSize;
	}

	public void map() {
		File file = new File(
				"/home/bsendir1/workspacemarla/FileSplitter/input/materials_dbv2-04052013.json");

		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(file, "r");
			System.out.println(file.length());
			raf.setLength(file.length());
			FileChannel chan = raf.getChannel();
			long t0 = System.currentTimeMillis();
			System.out.println(raf.length());
			long chunkOff = 0;
			while (chunkOff < raf.length()) {
				MappedByteBuffer map;
				if ((chunkOff + chunkSize) > raf.length()) {
					chunkSize = raf.length() - chunkOff;
				}
				map = chan.map(MapMode.READ_ONLY, chunkOff, chunkSize);
				chunkOff += chunkSize;
				maps.add(map);

			}
			raf.close();
			long t1 = System.currentTimeMillis();
			System.out.println("Mapping Took: " + (t1 - t0) + "ms");
			System.out.println("Created " + maps.size());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void split() {
		int cores = Runtime.getRuntime().availableProcessors();
		ExecutorService e = Executors.newFixedThreadPool(cores);
		System.out.println("Executing with " + cores + " threads.");
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < maps.size(); i++) {
			e.execute(new SplitWorker(maps.get(i), i));

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
		MappedByteBuffer map;
		int split_num;

		public SplitWorker(MappedByteBuffer nmappedByteBuffer, int i) {
			map = nmappedByteBuffer;
			split_num = i;

		}

		public void run() {

			System.out.println(split_num + " starts workin");

			FileChannel wChannel;
			try {
				wChannel = new FileOutputStream(new File("splits/temp"
						+ split_num)).getChannel();
				wChannel.write(map.load().asReadOnlyBuffer());
				// maps.get(i).load().asReadOnlyBuffer()
				wChannel.close();
				// capacity gets map size
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
