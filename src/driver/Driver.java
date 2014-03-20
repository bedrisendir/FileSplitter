package driver;

import java.io.IOException;
import java.util.regex.Pattern;

import splitter.RegexSplitter;
import splitter.Splitter;

public class Driver {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// long blocksize=64L*1024*1024;
		// String html =
		// "foo <a href='link1'>bar</a> bar teafaljsfnalfasf \n lasfjlsa;fnsaf  <a href='link2'>qux</a> foo";
		
		Pattern p = Pattern.compile("<a href='(.*?)'>");
		
		// Matcher m = p.matcher(html);
		/*
		 * if(m.matches()){ System.out.println("ss"); }
		 * 
		 * while(m.find()) { System.out.println(m.group());
		 * System.out.println(m.group(0)); System.out.println(m.group(1)); }
		 */
		
		Splitter s = new RegexSplitter(p,10);
		s.make_splits();

		/*
		 * AsyncFileSplitter s=new AsyncFileSplitter(blocksize); s.map();
		 * s.split();
		 */
		/*
		 * LineSplitter s=new LineSplitter(); s.make_splits(); long t1 =
		 * System.currentTimeMillis(); System.out.println("Line Splitting: " +
		 * (t1 - t0) + "ms");
		 */
	}

}
