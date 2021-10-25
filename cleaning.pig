register /home/garry/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- load in movie and ratings tables

ratings = load 'raw_data/ml-latest-small/ratings.csv' using CSVLoader(',') as
(userID:int, movieID:int, rating:double, timestamp:int);
ratings = filter ratings by userID is not null;

movies = load 'raw_data/ml-latest-small/movies.csv' using CSVLoader(',') as
(movieID:int, title:chararray, genre:chararray);
movies = filter movies by movieID is not null;

-- clean the movies table

movies = foreach movies generate
        movieID,
        REGEX_EXTRACT(title, '\\((\\d+)\\)', 1) as year,
        REGEX_EXTRACT(title, '([\\S ]+) \\(\\d+\\)', 1) as title,
        STRSPLIT(genre, '\\|') as genre;

-- left join the ratings table with the movies table into a table called ratings_plus

ratings_plus = join ratings by movieID LEFT OUTER, movies by movieID;

-- dump ratings_plus;

-- save the ratings_plus table to a directory called cleaned_data

-- fs -rm -rf output/movies -- remove old dir
store ratings_plus into 'cleaned_data/' using PigStorage('\t', '-schema');