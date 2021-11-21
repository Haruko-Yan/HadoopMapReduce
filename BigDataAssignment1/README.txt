This project is for COSC 2637/2633 Big Data Processing Assignment 1.

********************************************

Where is the jar files?

The jar files is in the 'target' folder. The standalone jar is called 'BigDataAssignment1-0.0.1-SNAPSHOT-shaded.jar'

********************************************

How to use the standalone jar file?

When you are in the hadoop platform, enter the following command for each task:

1. For task 1
hadoop jar BigDataAssignment1-0.0.1-SNAPSHOT-shaded.jar group.s3749857.BigDataAssignment1.Task1 <HDFS_input_folder> <HDFS_output_folder>
2. For task 2
hadoop jar BigDataAssignment1-0.0.1-SNAPSHOT-shaded.jar group.s3749857.BigDataAssignment1.Task2 <HDFS_input_folder> <HDFS_output_folder>
3. For task 3
hadoop jar BigDataAssignment1-0.0.1-SNAPSHOT-shaded.jar group.s3749857.BigDataAssignment1.Task3 <HDFS_input_folder> <HDFS_output_folder>

********************************************

Task 4

The performance analysis for task 2(using in-mapper combining without preserving state across documents) and task 3(using in-mapper combining with preserving state across documents):

For analysing the performance of them, I used the text files called 3littlepigs.txt, Melbourne.txt and RMIT.txt and their k copies(k = 0, 3, 6, 9) as the input files. 
I used the sum of output records from Mapper(the total number of key-value pairs output from Mapper) as the performance indicator. Below is the sum of output records from Mapper using 3 text files and their k copies:

	without preserving state across documents	with preserving state across documents
k = 0			13213					6206
k = 3			52852					24824
k = 6			92491					43442
k = 9			132130					62060

The test result shows that 
1. The total number of key-value pairs output from Mapper without preserving state across documents is greater than that with preserving state across documents.
2. the total number of key-value pairs output from Mapper without preserving state across documents increases faster than that with preserving state across documents when k increases.

The result indicates that the performance in producing intermediate results can be improved more significant when there are more input files.


