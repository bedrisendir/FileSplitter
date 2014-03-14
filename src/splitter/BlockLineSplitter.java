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




public class BlockLineSplitter {
	ArrayList<MappedByteBuffer> maps = new ArrayList<MappedByteBuffer>();
	long chunkSize;
	long offset=0;
	byte[] lf= System.getProperty("line.separator").getBytes();
	
	
	public BlockLineSplitter(long nChunkSize) {
		chunkSize = nChunkSize;
	}

	public void map() {
		File file = new File(
				"/import/linux/home/bsendir1/FileSplitter/FileSplitter/input/materials_dbv2-04052013.json");
		try {
			RandomAccessFile raf;
			raf = new RandomAccessFile(file, "rw");
			System.out.println(file.length());
			raf.setLength(file.length());
			FileChannel chan = raf.getChannel();
			long t0 = System.currentTimeMillis();
			System.out.println(raf.length());
		
			while (offset < raf.length()) {
				MappedByteBuffer map;
				if ((offset + chunkSize) > raf.length()) {
					chunkSize = raf.length() - offset;
				}
				System.out.println("   " + chan.position());
				long diff=findNextLine(raf);
				System.out.println("DIFF "+ diff +"   " + chan.position());
				map = chan.map(MapMode.READ_WRITE, offset, chunkSize+diff);
				offset = offset+ chunkSize+ diff;
				System.out.println("OFFSET "+ offset );
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
	private long findNextLine(RandomAccessFile raf) throws IOException{
		raf.seek(offset+chunkSize);
		long old=raf.getFilePointer();
		System.out.println("Pre:" + raf.getFilePointer());
		raf.readLine();
		System.out.println("Post:"+raf.getFilePointer());
		long newp = raf.getFilePointer();
		return newp - old;
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
			if((split_num%10)==0){
				System.out.println(split_num+ "th task started");
			}
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
