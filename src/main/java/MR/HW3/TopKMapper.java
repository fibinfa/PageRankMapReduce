package MR.HW3;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * top 100 pages sorted based on page rank in decreasing order
 * @author fibinfa
 *
 */
public class TopKMapper extends Mapper<Object, Text, NullWritable, Text> {
	// Our output key and value Writables
		private TreeMap<Double, Text> repToRecordMap = new TreeMap<>();
		
		@Override
		public void map(Object key, Text value, Context context) {
			//parse each line
			String line = value.toString();
			String[] split = line.split("\t");
			String pageName = split[0];
			String[] adjListWithPR = split[1].split(",");
			Double pageRank = Double.parseDouble(adjListWithPR[adjListWithPR.length-1]); 
			repToRecordMap.put(pageRank, new Text(pageName+ ","+String.valueOf(pageRank)));
			//filter out top 100
			if (repToRecordMap.size() > 100) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
		//once all maps are executed we get local top 100 from each job
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(),t);
			}
		}

}
