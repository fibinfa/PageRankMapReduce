package MR.HW3;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * get local top 100 and consolidates it to get global top 100
 * @author fibinfa
 *
 */
public class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	
	private TreeMap<Double, Text> repToRecordMap = new TreeMap<>();
	
		@Override
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				//parse
				String pageName = value.toString().split(",")[0];
				Double pageRank = Double.parseDouble(value.toString().split(",")[1]);
				repToRecordMap.put(pageRank, new Text(pageName+" "+String.valueOf(pageRank)));
				
				//removes the lowest key
				if (repToRecordMap.size() > 100) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
			int rank =1;
			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), new Text(rank+" " + t.toString()));
				rank++;
			}
			
		}
}
