package driver;

import java.io.IOException;

import splitter.AsyncFileSplitter;
import splitter.BlockLineSplitter;
import splitter.BlockSplitter;
import splitter.LineSplitter;
import splitter.Splitter;
import splitter.XMLSplitter;
import splitter.ZeroCopyLineSplitter;

public class Driver {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		ZeroCopyLineSplitter s=new ZeroCopyLineSplitter(128L*1024*1024);
		s.map();
		s.split();
		
	    /*
		LineSplitter s=new LineSplitter();
		s.make_splits();
		long t1 = System.currentTimeMillis();
		System.out.println("Line Splitting: " + (t1 - t0) + "ms");
	    */
	}

}
