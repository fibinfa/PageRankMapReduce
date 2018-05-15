package MR.HW3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import MR.HW3.PageRankDriver.COUNTERS;

/**
 * Mapper for calulating the page rank executed right after initialising 
 * nodes with page rank as 1/N
 * @author fibinfa
 *
 */
public class PageRankMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();

		// Extracts page rank from input text
		String[] initialSplit = line.split("\t");
		String[] adjListWithPR = initialSplit[1].split(",");
		Double pageRank = Double.parseDouble(adjListWithPR[adjListWithPR.length-1]);
		//calculate contribution from dangling node 
		String oldDelta = context.getConfiguration().get("previousDanglingContribution");
		double oldDeltaValue = (Double.parseDouble(oldDelta))/(Math.pow(10, 5));
		double newPageRank = pageRank+ (1-PageRankDriver.DAMPING_FACTOR)* 
				(oldDeltaValue/Integer.parseInt(context.getConfiguration().get("nodeCount")));
		
		String pageName = initialSplit[0];
		List<String> adjList;
		if(!adjListWithPR[0].contains("[]")) {
			//non-dangling nodes
			adjList = Arrays.asList(Arrays.copyOfRange
				(adjListWithPR, 0, adjListWithPR.length - 1));
		} else {
			//dangling nodes
			adjList = new ArrayList<>();
		}
		Node n = new Node(pageName, adjList);
		//pass along the graph structure
		context.write(new Text(n.getName()), new Text(n.toString()+","+newPageRank));
		
		if(adjList.size()>0) {
			//calculating contribution of inlinks
			double p = newPageRank/n.getLinks().size();
			for(String s: adjList) {
				context.write(new Text(s), new Text(String.valueOf(p)));
			}
		} else {
			//calculate delta for next iteration
			context.getCounter(COUNTERS.deltaCounter).increment((long) (pageRank*Math.pow(10, 5)));
		}
	}
}
