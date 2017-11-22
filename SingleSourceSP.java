package comp9313.ass2;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




/**
* COMP9313 Project 2
*   Single Source Shortest Path
* Author: Xinhong WU
* ZID: z5089853
* 
*/

public class SingleSourceSP {

    // used to store three parameters
    public static String OUT = "output";
    public static String IN = "input";
    public static String SOURCE = "source";

    // utilized a counter to check the termination criterion
    public static enum MYCOUNTER{
        COUNTER
    }


    /**
    * The Mapper class
    * 
    * The mapper class receive the input data, generate new node distance information based
    * on the adjacency list, and then emit the original data and the new generated data
    *
    * The input:
    *       first time: original file data
    *       else: the output from reducer
    * Data Format:
    *       input:
    *           first time: "EdgeId     FromNodeId      ToNodeId    Distance"
    *           else:       "TargetNodeID   Distance * AdjList * Path"
    *       output:
    *           "TargetNodeID   Distance * AdjList * Path"
    * 
    */
    
    public static class SSSPMapper extends Mapper<Object, Text, Text, Text> {
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // get the counter 
            // used to check whether it is the first time run mapper
            // int conuter = context.getConfiguration().getInt("run",1);
            SOURCE = context.getConfiguration().get("endNode");
            int nodeID = 0;
            String distAdjPath = null;
            String line = value.toString();
            String[] data = StringUtils.split(line);
            String distance = "Infinity";
            String adjList = "";
            String path = null;

            
            
            // judge whether this is the first time
            if (!line.contains("*")){
            	
                // receive the original file data
                // transform the data to the data format and then emit

                // set the sourceNode's distance to 0, others Infinity
                if(data[1].toString().equals(SOURCE)){
                    distance = "0.0";
                }else {
                    distance = "Infinity";
                }

                nodeID = Integer.parseInt(data[1].toString());
                path = "" + nodeID;
                adjList =  data[2] + ":" + data[3];
                distAdjPath = distance + "*" + adjList + "*" + path;
                context.write(new Text(Integer.toString(nodeID)),new Text(distAdjPath));
            }else{
                // receive the data from the reducer output
                // store the adjList information 
                // then emit the data
            	String[] d = StringUtils.split(line, "\t");
            	String[] dap = StringUtils.split(d[1], "*");
                distance = dap[0];
                
                if(dap.length == 3){
	                path = dap[2];
	                //String[] adjL = dap[1].split(", ");                
	                // stroe the adjacency list
	                adjList = dap[1];
                } else{
                	path = dap[1];
                	adjList = "";
                }
                // emit the data
                context.write(new Text(d[0]),new Text(d[1]));
            }

            if(distance.equals("Infinity")){
                return;
            }

            // for every pair of the adjacency list stored before
            // generate new data and then emit
            
            String[] adj = StringUtils.split(adjList, ", ");
            for (int i = 0; i < adj.length; i++){
                String[] a = adj[i].toString().split(":");
                String k = a[0];
                String j = a[1];
                double add = Double.parseDouble(j)+Double.parseDouble(distance);
                String v = String.format("%.1f", add);
                
                String outputValue = v + "*" + "*" + path + "->" + k;
                context.write(new Text(k),new Text(outputValue));
            }
       }

    }



    /**
    * The Reducer class
    * 
    * The reducer class received the data from the output of the mapper, update the distance
    * based on the current data information. Then emit the updated data.
    *
    * Input and Output Data Format:
    *      "TargetNodeID   Distance * AdjList * Path"
    * 
    */
    public static class SSSPReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String minDistance = "Infinity";
            String dis = "Infinity";
            String adjList = "";
            String path = null;
            String d0 = "";
            // System.out.println("%%%%%%%%%%%%%%%%%");
            for (Text val: values){
            	String data = val.toString();  
            	// System.out.println(data);
            	String[] d = StringUtils.split(data, "*");
            	dis = d[0];                
            	String p = "";

                // check whether the data has adjList
                if (d.length == 2){
                	p = d[1];
                	
                } else {
                	p = d[2];
                	d0 = d[0];
                	String[] adjTemp = d[1].split(", ");
	                for (String adj : adjTemp){
	                    adjList = adjList + adj + ", "; 
	                }
                }
                // update the shortest distance
                // if update, increase counter by 1
                if(minDistance.equals("Infinity")){
                	minDistance = dis;
                	path = p;
                } else {
                	if(dis.equals("Infinity")){
                		;
                	} else if (Double.parseDouble(minDistance) > Double.parseDouble(dis)){
                		minDistance = dis;
                		path = p;
                	}
                }
                
            }
            if(!d0.equals("")){
	            if(!minDistance.equals("Infinity")){
	            	if(d0.equals("Infinity")){
	            		context.getCounter(MYCOUNTER.COUNTER).increment(1L);
	            	} else {
	            		if(Double.parseDouble(d0) > Double.parseDouble(minDistance)){
	            			context.getCounter(MYCOUNTER.COUNTER).increment(1L);
	            		}
	            	}
	            }
            }
            // emit the updated data
            String newValue = "" + minDistance + "*" + adjList + "*" + path;
            
            
            context.write(key, new Text(newValue));
        }
    }



    /**
    * The Main Function
    * 
    * received three parameters:
    *       1. the input folder containing the graph file
    *       2. the output folder storing the final result file
    *       3. the query source node ID
    * 
    * First step: iteratively run the MapReduce job to find the shortest distance and path
    * Second step: run MapReduce to convert the output to desired format 
    *  
    */
    public static void main(String[] args) throws Exception {        

        // stored the three parameters
        IN = args[0];
        OUT = args[1];
        SOURCE = args[2];
        String input = IN;
        // generate several intermediate outputfile with differnt nanoTime
        int iteration = 0;
        String output = OUT + iteration;
        boolean isdone = false;
        Configuration conf = new Configuration();
        // iteratively run the MapReduce job to find the shortest distance and path
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        while(isdone == false){
            // run the MapRecuce job
            // Configuration conf = new Configuration();
            conf.set("endNode", SOURCE);
            // check if it is first time run

            Job job = Job.getInstance(conf,"SingleSourceSP");
            job.setJarByClass(SingleSourceSP.class);
            job.setMapperClass(SSSPMapper.class);
            job.setReducerClass(SSSPReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            
            // makes the last output become the next time input
            input = output;           
            // generate several intermediate outputfile with differnt nanoTime
            iteration++;
            output = OUT + iteration;
            // check if one of the map reduce have completed
            // job.waitForCompletion(true);
            // System.out.println("Iteration: "+iteration);
            if (job.waitForCompletion(true)){
            	// delete the intermediate output
            	if(iteration > 1){
    	            int deleteOutput = iteration - 2;
    	            String deleteFilePath = OUT + deleteOutput;
    	            fs.delete(new Path(deleteFilePath), true);
                }
                // check whether the counter has changed
                // if not changed, the iteration of MapRecuce is finished
                // make isdone to true
            	// System.out.println("Counter: "+job.getCounters().findCounter(MYCOUNTER.COUNTER).getValue());
            	if (job.getCounters().findCounter(MYCOUNTER.COUNTER).getValue() == 0){
            		isdone = true;
            	} 
            }

  
        }

        // use MapReduce to output the final result
        Configuration finalCONF = new Configuration();
        finalCONF.set("endNode",SOURCE);
        Job finalJOB = Job.getInstance(finalCONF,"SingleTargetSP");
        finalJOB.setJarByClass(SingleSourceSP.class);
        finalJOB.setMapperClass(FinalMapper.class);
        finalJOB.setReducerClass(FinalReducer.class);
        finalJOB.setMapOutputKeyClass(IntWritable.class);
        finalJOB.setMapOutputValueClass(Text.class);
        //set the number of the Reducers to 1;
        finalJOB.setNumReduceTasks(1);
        FileInputFormat.addInputPath(finalJOB, new Path(input));
        FileOutputFormat.setOutputPath(finalJOB, new Path(OUT));
        // delete the intermediate output
        if (finalJOB.waitForCompletion(true)){
        	fs.delete(new Path(input), true);
        }
        System.exit(finalJOB.waitForCompletion(true)?0:1);
        
        
        
    }


    /**
    * The FinalMapper Class And The FinalReducer Class
    * These two class is for the final output part. The data format of the output from
    * iteration part is not as required. These two class run a final MapReduce job to 
    * convert the output to the desired format.
    * 
    * Input Data Format:
    *      "TargetNodeID   Distance * AdjList * Path"
    * Output Data Format:
    *      "TargetNodeID    Distance     Path"
    * 
    */
    public static class FinalMapper extends Mapper<Object, Text, IntWritable, Text>{
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
        	
        	String line = value.toString();
            String[] sp = StringUtils.split(line, "\t");
            int nodeID = Integer.parseInt(sp[0]);
            String dap = sp[1];
            String distance = StringUtils.split(dap, "*")[0];
            String path = "";
            if(StringUtils.split(dap, "*").length == 2){
            	path = StringUtils.split(dap, "*")[1];
            } else{
            	path = StringUtils.split(dap, "*")[2];
            }
            String dp = distance + "\t" + path;
            context.write(new IntWritable(nodeID), new Text(dp));  
            
        }
    }
    public static class FinalReducer extends Reducer<Object, Text, IntWritable, Text>{
        public void reduce (Object key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            for (Text val:value){
            	String distance = StringUtils.split(val.toString(), "\t")[0];
                if (!distance.equals("Infinity")){
                	int outputKey = Integer.parseInt(key.toString());
                	context.write(new IntWritable(outputKey), new Text(val));
                }       
            }
        }  
   }

}

