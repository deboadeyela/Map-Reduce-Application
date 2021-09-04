# Map-Reduce-Application
 The program can be passed a list of text files to be processed via the command line. A number of large text or log files is used to test your program. 
 
 The threading strategy that was used for the mapping phase works as follows: The number 
of characters per thread is specified as an argument in the command line and an arrayList of that 
size is initialised, this arrayList is used to store the characters of the contents. Each index of the 
arrayList holds a fixed length of characters that were specified at the command line, and for each of 
the arrayList index’s a new thread is started, and their contents are read.

The threading strategy that was adopted for the reducing phase proceeds as follows: The 
number of words to use per thread is specified as an argument in the command line. In the java file 
the words are added to an arrayList of words until the specified size is reached - when the iteration 
number modulo words to be used in each thread is 0 then a new thread is started and all the words 
that were added to the thread from the last thread’s initialisation to the current iteration are used in 
that single thread for the reducing phase, once each word has been passed into the reducing phase 
the arrayList of words is cleared and the process repeats until there are no remaining words.

Both strategies also include indexes for the number of threads used in total. 
