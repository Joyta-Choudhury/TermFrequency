//Joyta Choudhury
//jchoudh1@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " wordcount ");			//to get the configuration object for this instance
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);				//adding input path
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));			//setting output path
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private final static DoubleWritable one  = new DoubleWritable( 1);
      private String delimit = "#####";      //delimiter used so that the output is of the form word#####filename
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

	FileSplit Split = (FileSplit)context.getInputSplit();           //InputSplit represents the data to be processed by an individual Mapper
	String file_name = Split.getPath().getName();                   //to get the file name of the current file

         String line  = lineText.toString();				//to convert the Text object to a string
         Text currentWord  = new Text();

         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
	    word = word+delimit+file_name;                            //so the output is of the form word#####filename
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
	 double termFreq = 0;  							//double as it has to be of the form '1.0'
         for ( DoubleWritable count  : counts) {  				//DoubleWritable values for loop
            sum  += count.get();
         }
	 termFreq = (1 + Math.log10(sum)); 					//to calculate the log values
         context.write(word,  new DoubleWritable(termFreq)); 	// to write the word with file name(delimiter and fine name) and the term frequency of the word
      }
   }
}
