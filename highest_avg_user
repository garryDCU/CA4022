register /home/garry/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Load data
ratings = load 'raw_data/ml-latest-small/ratings.csv' using PigStorage(',') as (userId:int, movieId:int, rating:double, timestamp:int);
ratings = filter ratings by userId is not null; -- remove first line (headers)

-- Aggregate ratings
agg_ratings = group ratings by (userId, rating);
ratings_counts = foreach agg_ratings generate group.userId, group.rating, COUNT(ratings) as num_ratings;

-- average rating for each user
groups = group ratings_counts by userId;
user_avg_ratings = foreach groups {
    mul = foreach ratings_counts generate rating * num_ratings;
    generate group as userId, SUM(mul) / SUM(ratings_counts.num_ratings) as avg_rating;
};

user_avg_ratings = order user_avg_ratings by avg_rating desc;
user_avg_ratings = limit user_avg_ratings 1;
dump user_avg_ratings;
