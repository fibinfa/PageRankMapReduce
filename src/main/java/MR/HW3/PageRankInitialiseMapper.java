package MR.HW3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import MR.HW3.PageRankDriver.COUNTERS;

/**
 * For initialising page rank vlaues to be 1/N
 * This map only job is executed right after the pre-processing job
 * Value of N is obtained from the reduce job
 * @author fibinfa
 *
 */
public class PageRankInitialiseMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		//input to the map is each line from the file
		String line = value.toString();

		// Extracts page rank  from input line
		String[] initialSplit = line.split("\t");
		String[] adjListWithPR = initialSplit[1].split(",");
		double pageRank = 1.0/Integer.parseInt(context.getConfiguration().get("nodeCount"));
		String pageName = initialSplit[0];
		List<String> adjList;
		if(!adjListWithPR[0].contains("[]")) {
			//non dangling nodes
			adjList = Arrays.asList(Arrays.copyOfRange
				(adjListWithPR, 0, adjListWithPR.length - 1));
		} else {
			//dangling node
			adjList = new ArrayList<>();
		}
		//setting extracted info to node
		Node n = new Node(pageName, adjList);
		n.setPageRank(pageRank);
		//emit updated page rank
		context.write(new Text(n.getName()), new Text(n.toString()+","+String.valueOf(n.getPageRank())));
	}
}
