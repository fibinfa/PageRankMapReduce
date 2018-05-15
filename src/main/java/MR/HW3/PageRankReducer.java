package MR.HW3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce part of the page rank contribution
 * Reducer will receive node structure along with inlink contribution and previous page rank, which is used 
 * to calculate the new page rank
 * @author fibinfa
 *
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sum =0;
		Node currentNode = new Node();
		currentNode.setName(key.toString());
		for(Text text: values) {
			String p = text.toString();
			if(p.contains(",")) {
				//node object was found
				String[] adjListWithPR = p.split(",");
				List<String> adjList;
				if(!adjListWithPR[0].contains("[]")) {
					//non-dangling node
					adjList = Arrays.asList(Arrays.copyOfRange
						(adjListWithPR, 0, adjListWithPR.length - 1));
				} else {
					adjList = new ArrayList<>();
				}
				currentNode.setLinks(adjList);
			} else {
				//found p value(page rank contribution from inlink), adding it to the page rank
				sum+=Double.parseDouble(p);
			}
		}
		int N = Integer.parseInt(context.getConfiguration().get("nodeCount"));
		double alpha = PageRankDriver.DAMPING_FACTOR;
		//page rank calculation
		double newPageRank = (alpha/N) + (1-alpha)*sum;
		currentNode.setPageRank(newPageRank);
		//links not in the key handled here
		if(currentNode.getLinks()!=null)
			//emit page name along with new page rank values
		context.write(new Text(currentNode.getName()), 
				new Text(currentNode.toString()+","+
		String.valueOf(currentNode.getPageRank())));
	}
}
