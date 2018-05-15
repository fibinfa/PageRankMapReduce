
				
1 PageRank in MapReduce:

Files:
Source Code Mapreduce program without combiner:
	src -> main-> java -> MR -> HW3 
Main class : PageRankDriver.java

Preprocessor : 
		Mapper: PreprocessMapper.java
		Reducer: PreprocessReducer.java
PageRankCalculation:
		Mapper: PageRankMapper.java
		Reducer: PageRankReducer.java
Top100page:
		Mapper: TopKMapper.java
		Reducer TopKreducer.java

Parser: Bz2wikiparser.java


_________________________________________________________________________________________________________________________________

2 Local Execution using MakeFile

Steps to run java

   1. Go to source folder and download the make file
   2. Create input directory and put the input file
   3. Go to the directory and in the terminal Run the command
	> make alone 
   4. Check the output in the output directory

_________________________________________________________________________________________________________________________________

 Local Execution using Eclipse

Steps to run java

   1. Create a maven project.
   2. Create input directory for putting input files
   4. Update the pom.xml files with dependencies and rebuild the maven project.
   5. Go to Run Configuration and provide the input file path and output filepath as argument.
   6. Run the program
   7. Check the output in the output directory
_________________________________________________________________________________________________________________________________


3 PageRank AWS Execution

Steps to Run Program in cloud

   1. Update the make file with changes to input, output, s3bucketname, subnetid and job.name
   2. Mention the number of nodes required as 6 or 11
   3. Run these commands in the terminal :
		make-bucket
		upload-input-aws
		make cloud
   5. Check the output in the output directory mentioned in aws
   6. Check the log files in log folder of s3 bucket mentioned


_________________________________________________________________________________________________________________________________

