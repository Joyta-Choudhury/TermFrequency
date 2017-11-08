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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Set;
import java.util.*;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;

public class Rank extends Configured implements Tool {

  private static final Logger LOG = Logger .getLogger( Rank.class);
  
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
   }
	
   public int run( String[] args) throws  Exception {
	String user_query = "";
		for(int i = 4;i<args.length;i++)
		{
			user_query = user_query +" "+ args[i];                       //to get the user query from the command line arguments(terminal)
		}
       //implementing job1
      Job job1  = Job .getInstance(getConf(), "wordcount");
      job1.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job1,  args[0]);                   		//to set input path 
      FileOutputFormat.setOutputPath(job1,  new Path(args[ 1]));		//to set output path
      job1.setMapperClass( Map .class);
      job1.setReducerClass( Reduce .class);
      job1.setOutputKeyClass( Text .class);
      job1.setOutputValueClass( DoubleWritable .class);
      job1.waitForCompletion(true);

      FileSystem file_system = FileSystem.get(getConf());
      ContentSummary content_summary = file_system.getContentSummary(new Path(args[0]));           //To Store the summary of a content
      long count_files = content_summary.getFileCount();
      Configuration conf = new Configuration();
      conf.setLong("countfiles",count_files);                              //passing the number of files to the job through the configuration using set
      Configuration conf1 = new Configuration();
      conf1.set("query", user_query);					   //passing the query entered by the user using set
      Configuration confRank = new Configuration();

	//implementing job2
	Job job2  = Job .getInstance(conf, " word_count_inv_frq ");
	job2.setJarByClass(this.getClass());
	FileInputFormat.addInputPath(job2, new Path(args[1]));		//to add input path
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));	// to set output path - intermediate file (file created after TFIDF.java)
	job2.setMapperClass(Map_IDF.class);
	job2.setReducerClass(Reduce_IDF.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	job2.waitForCompletion(true);

	//implementing job3
	Job job3 = Job .getInstance(conf1, "Search_Words");
	job3.setJarByClass(this.getClass());
	FileInputFormat.addInputPath(job3, new Path(args[2]));		//to add input path
	FileOutputFormat.setOutputPath(job3, new Path(args[3]));	//to set output path - intermediate file (file created after Search.java)
	job3.setMapperClass(Map_Search.class);
	job3.setReducerClass(Reduce_Search.class);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(DoubleWritable.class);
	job3.waitForCompletion(true);
	
	//implementing job4
	Job job4 = Job .getInstance(confRank, "Rank_Words");
	job4.setJarByClass(this.getClass());
	FileInputFormat.addInputPath(job4, new Path(args[3]));		//to add input path
	FileOutputFormat.setOutputPath(job4, new Path(args[4]));	// to set output path - final output file after Rank.java
	job4.setMapperClass(Map_Rank.class);
	job4.setReducerClass(Reduce_Rank.class);
	job4.setOutputKeyClass(Text.class);
	job4.setOutputValueClass(Text.class);
	return job4.waitForCompletion( true)  ? 0 : 1;

   }
 public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private final static DoubleWritable one  = new DoubleWritable( 1);
      private String delimit = "#####";                          //delimiter used so that the output is of the form word#####filename
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

	FileSplit Split = (FileSplit)context.getInputSplit();    //InputSplit represents the data to be processed by an individual Mapper
	String file_name = Split.getPath().getName();           //to get the file name of the current file

         String line  = lineText.toString();
         Text currentWord  = new Text();

         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
	    word = word+delimit+file_name;                     //so the output is of the form word#####filename
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
         for ( DoubleWritable count  : counts) {  //DoubleWritable values for loop
            sum  += count.get();
         }
	 termFreq = (1 + Math.log10(sum)); 			//to calculate the log values
         context.write(word,  new DoubleWritable(termFreq)); // to write the word with file name(delimiter and fine name) and the term frequency of the word
      }
   }
public static class Map_IDF extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      
      private Text word  = new Text();
      public void map( LongWritable key,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
	 String[] keypair = line.split("#####");                 // to split it via delimiter
	 String[] valuepair = keypair[1].split("\\s+");
	 context.write(new Text(keypair[0]), new Text(valuepair[0]+"="+valuepair[1]));  
         }
}
      public static class Reduce_IDF extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
	public void reduce( Text word,  Iterable<Text> counts,  Context context)
	throws IOException,  InterruptedException {
			int counter =0;
			double inv_doc_freq=0;
			HashMap<String,String> hash_map = new HashMap<String, String>();
//hashmap to store the filenames and the tf scores for a particular word
			//to code to get the filecount in the collection
			long total_files_count =Long.parseLong(context.getConfiguration().get("countfiles"));
			for(Text count:counts){
				 String line = count.toString();
				 String[] file_param = line.split("=");   //seperating the filename and the term frequency score
				 hash_map.put(file_param[0],file_param[1]);
				 counter ++;                                    //adding the file name and the tf score of a particular word to the hashmap
			}
			inv_doc_freq = Math.log10(1+((double)total_files_count/counter)); 	//calculating the smooth IDF

			for (Entry<String, String> entry: hash_map.entrySet()){
				 String final_word = word.toString()+"#####"+entry.getKey();     //reading the values from the hashmap
				 String term_Freq = entry.getValue();
				 Double TF_IDF = (Double.parseDouble(term_Freq)) * inv_doc_freq;
				 context.write(new Text(final_word), new Text(TF_IDF.toString()));
			}
		}
	}
public static class Map_Search extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
    public void map( LongWritable offset,  Text lineText,  Context context)
      throws  IOException,  InterruptedException {
    	final Pattern WORD_BOUND = Pattern .compile("\\s*\\b\\s*");
			
			String line  = lineText.toString();
			String[] input_line = line.split("\\s");   
			String[] word_file = input_line[0].trim().split("#####");
			String userQuery = (context.getConfiguration().get("query"));//the query from the user
			
			for ( String word  : WORD_BOUND .split(userQuery)) {//to seperate the query according to the spaces
		          	  if (word.isEmpty()) {
		             		continue;
		          	  }
		          else { 
				  if (word.equals(word_file[0])) {//to check if the query words exist in the files
		        	  Double result = 0.0;
				  String final_file = word_file[1];
				  result = Double.parseDouble(input_line[1]);//now it contains the tfidf for the word in wordfile[0]
				   //if args contain the wordfile[0] then write the file name and value to the output else ignore
				  context.write(new Text(final_file),new DoubleWritable(result));//<"file2.txt ", "0.30102999566”>  
		          }  
			}
		    }
		}
	}

	public static class Reduce_Search extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {//output will be of "file2.txt 0.60205999132"
    @Override 
    public void reduce( Text file,  Iterable<DoubleWritable > counts,  Context context)
       throws IOException,  InterruptedException {
			 double sum  = 0.0;
			 for ( DoubleWritable count  : counts) {
				sum  += count.get();//to add the scores of the query
			 }
			 context.write(file,  new DoubleWritable(sum));
		}
	}
  public static class Map_Rank extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		  public void map( LongWritable offset,  Text lineText,  Context context)
		    throws  IOException,  InterruptedException {
					
					String line  = lineText.toString();
					String[] input_line = line.split("\\s"); 
					String file_name = input_line[0];
					Double value = Double.parseDouble(input_line[1]);
					String key_name = "rank_file";
					String intermediate_value = file_name+"#####"+value;
					context.write(new Text(key_name),new Text(intermediate_value));
		}  
	}

	public static class Reduce_Rank extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {//output will be of "file2.txt 0.60205999132"
		  @Override 
		  public void reduce( Text file,  Iterable<Text > counts,  Context context)
		     throws IOException,  InterruptedException {
			 
			 TreeMap<Double, String> tree_map = new TreeMap<Double, String>();//treemap is used to store the values
			 for(Text count:counts){
					String[] file_param = count.toString().trim().split("#####");
					String file_name1= file_param[0];
					Double value1 = Double.parseDouble(file_param[1]);
					tree_map.put(value1,file_name1);
			}

      		NavigableMap<Double, String> nav_map = tree_map.descendingMap();
			for (Entry<Double, String> entry: nav_map.entrySet()){
				 String file_name = entry.getValue();
				 Double value = entry.getKey();
				 context.write(new Text(file_name),  new DoubleWritable(value));//writes the filename and scores in descending order
			}
		}
	}
}

