# Illumio-assessment

# Task: </br>
Task 1: Description: Write a program that reads a file and finds matches against a predefined set of words. There can be up to 10K entries in the list of predefined words.  </br>
Requirement details : i) Input file is a plain text (ascii) file, with every record separated by a new line.  </br>
ii) For this exercise, assume English words only  </br>
iii) The file size can be up to 20 MB  </br>
iv) The predefined words are defined in a text file, every word separated by a newline. Use a sample file of your choice for the set of predefined keywords for the exercise.  </br>

# Assumptions:
i) I have assumed that each record in the input file is a word separated by a new line.  </br>
ii) All the words are case-sensitive  </br>
iii) Regarding the matching, I assume to filter all the words from the input file with the predefined words and save them onto a new file.  </br>

# prerequisites:
Python 3.0  </br>
Pyspark  </br>

# File description:
Actual code - Illumio_Assessment.ipynb  </br>
Input file: input.txt (~20 MB)  </br>
Pre-defined words - predefined_words.txt (~10k words)  </br>
Output files: output.txt (Regular implementation), output-spark folder (spark implementation)  </br>

# Methodologies:
I have implemented two methods to achieve this task in order to compare the performance.

  # 1. Regular Python implementation:
  First, I have read all the predefined words into a dictionary as it provides an efficient lookup for future operations. 
  Then, I have read the input file data (all lines) and then filtered the words based on their existence in the dictionary. 
  Here, in order to avoid multiple writes to the file, I have appended all the matched words and finally wrote them to the file.

    Complexity: if m is the count of predefined words and n is the count of words in the input file then, 
              time complexity = O(m + n) assuming that it takes O(1) time for dictionary lookup.
              space complexity = O (m + n)*s where s is the average length of the word.
              
  Other considerations: Here, I was able to read all the input file data (20MB) at a time and filter them because of system memory. 
                      However, if the data size increases, we might not be able to load the entire at once. so we probably can read 
                      and write the data in batches.

 # 2. Spark Implementation:
  Since, I already worked with Spark for large data processing, thought of comparing its performance for this task. 
  In this implementation, I joined both the data frames with an inner join to find the common words and then saved the filtered words.

# Result:
For the input data with 20 MB, the regular program performed better compared to the spark implementation by almost 0.5s. However, when I tested the same code with a larger input size of almost (250MB) the spark achieved a better performance of 5s. So, I believe Spark can be efficient for processing larger data. However, based on the requirments of this task, general Python implementation is good enough.
