DROP TABLE IF EXISTS ratings PURGE;
CREATE TABLE IF NOT EXISTS ratings
(userID INT, movieID INT, rating DOUBLE, tim INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH 'raw_data/ratings.csv'
OVERWRITE INTO TABLE ratings;

DROP TABLE IF EXISTS movies PURGE;
CREATE TABLE IF NOT EXISTS movies
(movieID INT, title STRING, year INT, genre STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH 'cleaned_data/movies.csv'
OVERWRITE INTO TABLE movies;

set hive.cli.print.header=true;

-- movie with highest number of ratings

DROP VIEW IF EXISTS highestnumberratings;
CREATE VIEW highestnumberratings AS
SELECT movieID, COUNT(rating) AS ratingCount,
FROM ratings
GROUP BY movieID;

DROP VIEW IF EXISTS topMovie;
CREATE VIEW topMovie AS
SELECT m.movieID, m.title, ratingCount
FROM highestnumberratings t JOIN movies m ON t.movieID = m.movieID;

INSERT OVERWRITE LOCAL DIRECTORY '/home/garry/output/hive_analysis/highestRatedMovie'
SELECT *
FROM topMovie t JOIN movies m ON t.movieID = m.movieID
ORDER BY ratingCount DESC
LIMIT 1;

-- most liked movie 

DROP VIEW IF EXISTS highestnumber5ratings;
CREATE VIEW highestnumber5ratings AS
SELECT movieID, COUNT(rating) AS ratingCount,
FROM ratings
WHERE rating=5.0
GROUP BY movieID;

DROP VIEW IF EXISTS fiveStarMovie;
CREATE VIEW fiveStarMovie AS
SELECT m.movieID, m.title, ratingCount
FROM highestnumber5ratings t JOIN movies m ON t.movieID = m.movieID;

INSERT OVERWRITE LOCAL DIRECTORY '/home/garry/output/hive_analysis/fiveStarMovie'
SELECT *
FROM fiveStarMovie t JOIN movies m ON t.movieID = m.movieID
ORDER BY fiveStarCount DESC
LIMIT 1;

-- users with highest avg rating

INSERT OVERWRITE LOCAL DIRECTORY '/home/garry/output/hive_analysis/userHighestAvgRatings' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
SELECT userID, AVG(rating) as avgRating
FROM ratings
GROUP BY userID
ORDER BY avgRating DESC
LIMIT 1;

-- count number of ratings for each star level 

DROP VIEW IF EXISTS ratingCounts;
CREATE VIEW IF NOT EXISTS ratingCounts AS
SELECT movieID,
 COUNT(case when 4<rating and rating<=5 then 1 else null end) AS fiveStarRatingCount,
 COUNT(case when 3<rating and rating<=4 then 1 else null end) AS fourStarRatingCount,
 COUNT(case when 2<rating and rating<=3 then 1 else null end) AS threeStarRatingCount,
 COUNT(case when 1<rating and rating<=2 then 1 else null end) AS twoStarRatingCount,
 COUNT(case when 0<rating and rating<=1 then 1 else null end) AS oneStarRatingCount,
 COUNT(rating) AS ratingCount
FROM ratings
GROUP BY movieID;



