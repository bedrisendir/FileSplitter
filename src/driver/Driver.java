package driver;

import splitter.BlockLineSplitter;
import splitter.BlockSplitter;
import splitter.LineSplitter;
import splitter.Splitter;
import splitter.XMLSplitter;

public class Driver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//BlockLineSplitter s=new BlockLineSplitter(128L*1024*1024);
		//s.map();
		//s.split();
		
	    
		long t0 = System.currentTimeMillis();
		LineSplitter s=new LineSplitter();
		s.make_splits();
		long t1 = System.currentTimeMillis();
		System.out.println("Line Splitting: " + (t1 - t0) + "ms");
	    
	}

}
