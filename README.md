A spark project which analyses data in movielens data and updates the deltalake tables
The csv output gets saved in  output folder under ml-latest-small under data
Following fucntionality is achieved
Loading the tables from csv files
Merging the rating tables based on userid and movieId
Creating the ratings table with partition as date on which the job has run
identifying the top 10 movies
Saving the output in a csv file
