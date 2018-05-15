package MR.HW3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer will combine all the adj list of corresponding nodes and 
 * calculate the node count
 * @author fibinfa
 *
 */
public class PreProcessingReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder output = new StringBuilder();
		for(Text str: values) {
			if(!str.toString().contains("DummyAdjList"))
			output.append(str.toString()).append(",");
		}
		//links that are not in the key but in the adj list of other nodes
		// treat them as dangling node
		if(output.toString().length()==0)
			output.append("[],");
		//initial page rank as 0
		output.append("0");
		context.write(key, new Text(output.toString()));
		context.getCounter(PageRankDriver.COUNTERS.nodeCount).increment(1);
	}

}

