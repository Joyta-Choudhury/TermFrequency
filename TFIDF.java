//Joyta Choudhury
//jchoudh1@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Set;
import java.util.*;


public class TFIDF extends Configured implements Tool {

  private static final Logger LOG = Logger .getLogger( TFIDF.class);
  
   public static void main( String[] args) throws  Exception {
      //ToolRunner.run(new TermFrequency(),args);	//this is used for chaining while using eclipse but when tried with terminal it showed up with errors
      int res  = ToolRunner .run( new TFIDF(), args);
      System .exit(res);
   }
	
   public int run( String[] args) throws  Exception {
       //implementing job1
      Job job1  = Job .getInstance(getConf(), "wordcount");		//to get the configuration object for this instance
      job1.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job1,  args[0]);			//adding input path
      FileOutputFormat.setOutputPath(job1,  new Path(args[ 1]));	//setting output path
      job1.setMapperClass( Map .class);
      job1.setReducerClass( Reduce .class);
      job1.setOutputKeyClass( Text .class);
      job1.setOutputValueClass( DoubleWritable .class);
      job1.waitForCompletion(true);

      FileSystem file_system = FileSystem.get(getConf());
      ContentSummary content_summary = file_system.getContentSummary(new Path(args[0]));   //To Store the summary of a content (a directory or a file)
      long count_files = content_summary.getFileCount();
      Configuration conf = new Configuration();
      conf.setLong("countfiles",count_files);            //set the parameter using conf.setLong "countfiles"

	//implementing job2
	Job job2  = Job .getInstance(conf, " word_count_inv_frq ");		//to get the configuration object for this instance
	job2.setJarByClass(this.getClass());
	FileInputFormat.addInputPath(job2, new Path(args[1]));			//adding input path
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));		//setting output path
	job2.setMapperClass(Map_IDF.class);
	job2.setReducerClass(Reduce_IDF.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);

      return job2.waitForCompletion( true)  ? 0 : 1;
   }
     public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private final static DoubleWritable one  = new DoubleWritable( 1);
      private String delimit = "#####";                      //delimiter used so that the output is of the form word#####filename
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

	FileSplit Split = (FileSplit)context.getInputSplit();//InputSplit represents the data to be processed by an individual Mapper
	String file_name = Split.getPath().getName();//to get the file name of the current file

         String line  = lineText.toString();			//to convert the Text object to a string
         Text currentWord  = new Text();

         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
	    word = word+delimit+file_name;  		//so the output is of the form word#####filename
            currentWord  = new Text(word);
            context.write(currentWord,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
	 double termFreq = 0;  			//double as it has to be of the form '1.0'
         for ( DoubleWritable count  : counts) {  		//DoubleWritable values for loop
            sum  += count.get();
         }
	 termFreq = (1 + Math.log10(sum)); 		//to calculate the log values
         context.write(word,  new DoubleWritable(termFreq)); 	// to write the word with file name(delimiter and fine name) and the term frequency of the word
      }
   }
public static class Map_IDF extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      
      private Text word  = new Text();
      public void map( LongWritable key,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();			//to convert the Text object to a string
	 String[] keypair = line.split("#####");		//using split to get the values in two arrays namely key pair and value pair
	 String[] valuepair = keypair[1].split("\\s+");
	 context.write(new Text(keypair[0]), new Text(valuepair[0]+"="+valuepair[1]));	//to write the file in the form <"yellow ", "file2.txt=1.0">
         }
}
      public static class Reduce_IDF extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
	public void reduce( Text word,  Iterable<Text> counts,  Context context)
	throws IOException,  InterruptedException {
			int counter =0;
			double inv_doc_freq=0;
			HashMap<String,String> hash_map = new HashMap<String, String>();//hashmap to store the filenames and the termfreq scores for a particular word
											
			long total_files_count =Long.parseLong(context.getConfiguration().get("countfiles"));   //code to get the 'countfiles' using get
			for(Text count:counts){
				 String line = count.toString();
				 String[] file_param = line.split("=");   	//seperating the file name and the termfreq score
				 hash_map.put(file_param[0],file_param[1]);
				 counter ++;                                    //adding the file name and the termfreq score of a particular word to the hashmap
			}
			inv_doc_freq = Math.log10(1+((double)total_files_count/counter)); 	//calculating the smooth IDF

			for (Entry<String, String> entry: hash_map.entrySet()){
				 String final_word = word.toString()+"#####"+entry.getKey();     //reading the values from the hashmap and using delimiter in between 
				 String term_Freq = entry.getValue();
				 Double TF_IDF = (Double.parseDouble(term_Freq)) * inv_doc_freq;		// calculating TFIDF
				 context.write(new Text(final_word), new Text(TF_IDF.toString())); //to write the values of the form 'is#####file2.txt 0.30102999566'

			}
		}
	}
}
