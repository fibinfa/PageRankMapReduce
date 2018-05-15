package MR.HW3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;

/**
 * preprocessing mapper
 * reads line by line from input and creates a graph structure
 * @author fibinfa
 *
 */
public  class PreProcessingMapper extends Mapper<Object, Text, Text, Text> {
	private Text nodeId = new Text();
	private Text adjList = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		try {
			Node node = Bz2WikiParser.parseLine(value.toString());
			if(node!=null) {
			nodeId.set(node.getName());
			//remove duplicates from adj list
			Set<String> set = new HashSet<>();
			for(String s: node.getLinks()) {
				set.add(s);
			}
			List<String> newLinks = new ArrayList<>(set);
			node.setLinks(newLinks);
			adjList.set(node.toString());
			//for handling the case of nodes being in adj list but not in
			//key, treating them as dangling nodes
			for(String s: node.getLinks()) {
				context.write(new Text(s), new Text("DummyAdjList"));
			}
			context.write(nodeId, adjList);
			
			}
		} catch (ParserConfigurationException | SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
