1. Each record is read individually line by line from the file using the Pandas dataframe. 

2. Special conditions are checked in order to create new sessions: 

a. if it is a first row new session is assigned automatically;
b. if it is not previously encountered IP, new session is initiated;
c. if it is previously encountered IP, then inactivity time check is done using sessions dataframes which keeps track of the last access for each session

3. Session dataframe also contains session close times, which are evaluated on every iteration. If the difference between current time and last access is bigger than inactivity time, then session is closed and its closing time is recorded.

4. At the end the data is aggregated and summarized and the results are saved into the output file. 
