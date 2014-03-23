package driver;

import java.io.IOException;
import java.util.regex.Pattern;

import splitter.AsyncFileSplitter;
import splitter.BlockRegexSplitter;
import splitter.RegexSplitter;
import splitter.Splitter;
import splitter.XMLSplitter;
import splitter.ZeroCopyLineSplitter;

public class Driver {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		long blocksize=32L*1024*1024; //BlockSplitter + ZeroCopyLineSplitter + BlockLineSplitter + AsyncFileSplitter
		Pattern p = Pattern.compile("<a href='(.*?)'>"); //RegexSplitter
		String starttag="<dataset "; //XML Splitter
		String endtag="</dataset>"; //XML Splitter
		Splitter s=new BlockRegexSplitter(p,blocksize);
		s.make_splits();
	}

}
