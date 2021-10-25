register /home/garry/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Load data
ratings = load 'raw_data/ml-latest-small/ratings.csv' using PigStorage(',') as (userId:int, movieId:int, rating:double, timestamp:int);
ratings = filter ratings by userId is not null; -- remove first line (headers)

movies = load 'input/clean_data/movies.csv' using PigStorage('\t') as (movieId:int, year:int, title:chararray, genres:chararray);
movies = filter movies by movieId is not null; -- remove first line (headers)

-- Aggregate ratings
agg_ratings = group ratings by (movieId, rating);
ratings_counts = foreach agg_ratings generate group.movieId, group.rating, COUNT(ratings) as num_ratings;

-- Find movie with most number of ratings
movie_groups = group ratings_counts by movieId;
movie_total_ratings = foreach movie_groups generate group as movieId, SUM(ratings_counts.num_ratings) as num_ratings;
movie_total_ratings = order movie_total_ratings by num_ratings desc;
movie_total_ratings = limit movie_total_ratings 1;

-- join to other table to get the titles
title_movie = join movie_total_ratings by movieId left outer, movies by movieId;

dump title_movie;
