package driver;

import splitter.Splitter;
import splitter.XMLSplitter;

public class Driver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String start_tag=new String("<dataset ");
		String end_tag=new String("</dataset>");
		
		Splitter xml=new XMLSplitter(start_tag,end_tag);
		xml.make_splits();
	}

}
