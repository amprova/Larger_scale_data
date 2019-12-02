# Project2
# Amifa Raj

================================================================================
This project helped me to implement complex tasks using mapreduce. Thus it helped me to go deeper of this process.
I get to learn 
1. how to use multiple mapper and reducer
2. how to use multiple files as input

The use of multiple mapper and reducer was kind of tricky and it took some time to fully understand it.

# Dataset: 
I used rating data sets from the MovieLens website (http://movielens.umn.edu). The data sets were collected over various 
periods of time depending on the size of the set.
MovieLens 1M - Consists of 1 million ratings from 6000 users on 4000 movies.
It has 3 files 
ratings.dat: UserID::MovieID::Rating::Timestamp
movies.dat: MovieID::Title::Genres
user.dat: UserID::Gender::Age::Occupation::Zip-code

I also run these experiments on Movielens10M dataset and I almost same pattern in result. 
This data set contains 10000054 ratings and 95580 tags applied to 10681 movies by 71567 users.The ML10M dataset 
has more ratings but I haven't seen any significant difference in my results. I didn't use tag data.

## Insight I found interesting: 
First I started with simple experiments. With the experience of observing simple pattern I tried to find more 
interesting insight. Here I mentioned 6 insight from the dataset. Among them I found insight 3,4,5 most interesting.
I explained the insight and the result from those experiment. The algortihms are given at the end of the insights.

## 1st Insight: Popular movies of all time:
I used both ratings data and movies data to implement that. ratings data gave me total number of ratings per movieID. 
I joined that with the movie name from movies data. Thus I got topn popular movies of all time with name. 
I used three mapper class and two reducer class in total. 
The value of 'n' for generating topn is given by command line input.

The input format is: <input file> <input file> <topn> <output file>
Command	to run the jar file: hadoop jar TopKMovieName.jar TopKMovieName ratings.dat movies.dat 10 output

* Output Analysis: From the output I can see the topn movies based on number ratings of all time. 
The result seems expected. The movie 'American Beauty', 'Star Wars' and other popular movies are on the 
top 10 list which is not surprising. 
But for the ML10M dataset 'Pulp fiction' is the most popular movie though the movie released before 'American Beauty'. 
Also the list of top 10 movies are not that similar with the top 10 from ML1M dataset.

## 2nd Insight:  Popular Genre: 
Here I tried to find which genre is more popular and also which genre is less popular among the users.
For this reason I had to use both ratings data and movies data. For this I had to use 3 Mapper 
and 2 reducer class as well. 

The input format is: <input file> <input file> <output file>
>>Command to run the jar file: hadoop jar PopularGenre.jar PopularGenre ratings.dat movies.dat output

* Output Analysis: From the output I found that Comedy(356580 ratings) and Drama(354529) movies are the 
most popular among the users. But Documentary movies are the least rated genre. Only 7910 ratings.
For Movielense 10M dataset Drama is the most popular.

## 3rd Insight: Best and Worst Movies Per Genre: 
After observing the most and least popular genre I wanted to know which movies is the most popular in 
particular genre and also the least popular.
To do that I used both movies and ratings data which gave me the most and the least popular movie per genre.
For Movielens 10M dataset I found few movies that do not have genre information.
The input format is: <input file> <input file> <output file>
>>Command to run the jar file: hadoop jar PopularByGenre.jar PopularByGenre ratings.dat movies.dat output

* Output Analysis: I found few interesting facts from the output
	1. the number of unique movies in ratings data and number of uniques movies in movies data are not 
same. There are 177 movies which have no ratings. That is why some least popular movie have frequency zero 
which means they have no rating at all.
	2. Same movie can be the most popular in multiple genre
	3. The movie 'Star Wars: Episode VI - Return of the Jedi (1983)' is the most popular movie in 
'Romance' genre. 'American Beauty' is the most popular in both 'Drama' and 'Comedy' genre.
	4. For ML10M dataset 'Pulp Fiction' is the most popular in comedy genre.

## 4th Insight: Popular movie by year(Evergreen movies)
While working with the movies I saw that every movie has it's release date at the end of its title, 
so I decided to use that. The data has movies since 1919 to 2000. I tried to get the most popular movies of 
those years. I showed topn movies per year and n was given by the command line.
To do this I used both ratings and movies data. 
The input format is: <input file> <input file> <topn> <output file>
>>Command to run the jar file: hadoop jar Evergreen.jar Evergreen ratings.dat movies.dat 1 output

* Output Analysis: From the output I can see the oldest movie is from 1919 and the newest movie 
is from 2000. It is surprising that movies from that early were still popular at 2000. Though the 
top movies before 1930 do not have a lot of ratings it's still surprising that people 
watch movies that released before 1930. But for the dataset ML10M movies from the early 90s also have
a lot of ratings.

## 5th Insight: Worst most popular Movie
The idea of this observation is to get the movies which have highest number of rating value 1. 
User giving a movie rating 1 means the user did not like the movie. I wanted to see what movies 
have been watched a lot of times but disliked by the user. which indicates that the movie is 
popular but not good. So I took all the movies that have ratings 1 and count how many time they got that rating. 
To show the topn worst most popular movie I took the n from comman line input.
The input format is: <input file> <input file> <topn> <output file>
>>Command to run the jar file: hadoop jar WorstPopular.jar WorstPopular ratings.dat movies.dat 10 output

* Output Analysis: From the output I can see that for ML1M dataset the top worst movie has 314 rating 
where the rating value is 1 and for ML10M dataset the top one has 2269 rating 1.
I think if I had implicit rating from where I can know the watch history 
of the user, it will be meaningful to get the most watched movie with least rated value. Still it seems 
interesting to see which movies a lot of user started watching but end up disliking that.

## 6th Insight: Active user by demographics
I wanted to use demographic information of the users given in the user data. so I thought I will see 
which demographic group is more active in giving ratings to the movie. I observed this pattern for age 
and gender. For both observation the main logic of code was same. 
The input format is: <input file> <input file> <output file>
>>Command to run the jar file: hadoop jar ActiveAge.jar ActiveAge ratings.dat users.dat output
			       hadoop jar ActiveGender.jar ActiveGender ratings.dat users.dat output

* Output Analysis: From the output I found that Male are a lot more active than female in 
prividing ratings. Also the user between age range 25: "25-34" are the most active users and the 
users below 18 are least active

================================================================================
Algorithms for all the insights are given below- 

## Algorithm for insight 1: 
1. class MovieMapper extends Mapper<Object, Text, Text, Text>:
	1. takes the ratings.dat file as input value
	2. gets the movieID by splitting the data on "::"
	3. emit(MovieID, one)
2. class MovieNameMapper extends Mapper<Object, Text, Text, Text>:
	1. takes the movies.dat file as input value
	2. gets the movieID and MovieName by splitting the rows on "::"
	3. emit(MovieID, MovieName)
3. class MovieReducer extends Reducer<Text, Text, Text, Text>:
	1. gets input from both MovieMapper and MovieNameMapper Class
	2. iterating over the values:
	3.		if value is one
	4.			count++
	5.		else
	6.			MovieName = value
	7. emit(MovieName,count)
4. class MovieSort extends Mapper<LongWritable, Text, LongWritable, Text>:
	1. reads the intermediate output generated by 1st reducer
	2. reads the line number as key and (MovieName,count) as value
	3. tokenize the value by splitting on "\t"
	4. String s = count::MovieName
	5. gives a general key to all the values
	6. emit(one, s)
5. class SortReducer extends Reducer<LongWritable, Text, LongWritable, Text>
	1.  iterates over the value from the Mapper
	2. After splitting the value into token store the value in treemap where count is key and MovieName is the value
	3. emit(count, MovieName) until n

## Algorithm of Insight 2:
1. class MovieMapper extends Mapper<Object, Text, Text, Text>
	1. takes the ratings.dat file as input value
	2. gets the movieID by splitting the data on "::"
	3. emit(MovieID, one)
2. class MovieGenreMapper extends Mapper<Object, Text, Text, Text>
	1. takes the movies.dat file as input value
	2. gets the movieID and genre by splitting the rows on "::"
	3. emit(MovieID, Genre)
3. class MovieReducer extends Reducer<Text, Text, Text, Text>:
	1. gets input from both MovieMapper and MovieGenreMapper Class
	2. iterating over the values:
	3.		if value is one
	4.			count++
	5.		else
	6.			list of genre = value
	7. For each genre
	7. 		emit(Genre,count)
4. class MovieSort extends Mapper<LongWritable, Text, LongWritable, Text>
	1. reads the intermediate output generated by 1st reducer
	2. reads the line number as key and (Genre,count) as value
	3. emit(one, value)
5. class SortReducer extends Reducer<LongWritable, Text, Text, Text>
	1. reads key and values from the previous mapper
	2. iterating over the value
	3. 		gets genre and count by splitting the value on "\t"
	4. 		storing the genre as key and the total count as value in a hashmap
	5. 	for each genre
	6.		store the the count as key and genre as value in a treemap
	8. treemap stores the sorted list of genre by count
	7. emit(count, genre)

## Algortihm of Insight 3:
1. class MovieMapper extends Mapper<Object, Text, Text, Text>
	1. takes the ratings.dat file as input value
	2. gets the movieID by splitting the data on "::"
	3. emit(MovieID, one)

2. class MovieNameAndGenreMapper extends Mapper<Object, Text, Text, Text>
	1. takes the movies.dat file as input value
	2. gets the movieID, MovieName and genre by splitting the rows on "::"
	3. String MovieInfo = MovieName+Genre
	3. emit(MovieID, MovieInfo)
3. class MovieReducer extends Reducer<Text, Text, Text, Text>:
	1. gets input from both MovieMapper and MovieNameAndGenreMapper Class
	2. iterating over the values:
	3.	if value is one
	4.		count++
	5.	else
	6.		MovieInfo = value
	7. For each genre
	8. 		emit(MovieInfo,count)
4. class MovieSort extends Mapper<LongWritable, Text, Text, Text> 
	1. reads the intermediate output generated by 1st reducer
	2. reads the line number as key and (MovieInfo,count) as value
	3. gets the genre and MovieName by splitting the MovieInfo
	4. for each genre
	5. 	emit(genre, count+MovieName)
5. class SortReducer extends Reducer<Text, Text, Text, Text>
	1. reads key and values from the previous mapper
	2. iterating over the value
	3. 	gets count and MovieName by splitting the value
	4. 	storing the count as key and the MovieName as value in a treemap
	5. treemap stores the sorted list of movies by count
	6. emit(genre, most popular MovieName)
	7. emit(genre, least popular MovieName)

## Algorithm of Insight 4:
1. class MovieMapper extends Mapper<Object, Text, Text, Text>
	1. takes the ratings.dat file as input value
	2. gets the movieID by splitting the data on "::"
	3. emit(MovieID, one)

2. class MovieNameMapper extends Mapper<Object, Text, Text, Text>
	1. takes the movies.dat file as input value
	2. gets the movieID, MovieName and genre by splitting the rows on "::"
	3. String MovieInfo = MovieName
	3. emit(MovieID, MovieName)
3. class MovieReducer extends Reducer<Text, Text, Text, Text>:
	1. gets input from both MovieMapper and MovieNameMapper Class
	2. iterating over the values:
	3.	if value is one
	4.		count++
	5.	else
	6.		MovieName = value
	7. For each genre
	8. 	emit(MovieName,count)
4. class MovieSort extends Mapper<LongWritable, Text, Text, Text> 
	1. reads the intermediate output generated by 1st reducer
	2. reads the line number as key and (MovieName,count) as value
	3. gets the Year and MovieName by splitting the MovieName
	4. for each Year
	5. 	emit(Year, count+MovieName)
5. class SortReducer extends Reducer<Text, Text, Text, Text>
	1. reads key and values from the previous mapper
	2. iterating over the value
	3. 	gets count and MovieName by splitting the value
	4. 	storing the count as key and the MovieName as value in a treemap
	5. treemap stores the sorted list of movies by count
	6. For each entry until topn
	7. 	emit(genre, MovieName)

## Algorithm of Insight 5:
1. class MovieMapper extends Mapper<Object, Text, Text, Text>
	1. takes the ratings.dat file as input value
	2. gets the movieID by splitting the data on "::"
	3. if the rating is 1
	3. 	emit(MovieID, one)

2. class MovieNameMapper extends Mapper<Object, Text, Text, Text>
	1. takes the movies.dat file as input value
	2. gets the movieID, MovieName and genre by splitting the rows on "::"
	3. String MovieInfo = MovieName
	3. emit(MovieID, MovieName)
3. class MovieReducer extends Reducer<Text, Text, Text, Text>:
	1. gets input from both MovieMapper and MovieNameMapper Class
	2. iterating over the values:
	3.	if value is one
	4.		count++
	5.	else
	6.		MovieName = value
	7. For each genre
	8. 	emit(MovieName,count)
4. class MovieSort extends Mapper<LongWritable, Text, Text, Text> 
	1. reads the intermediate output generated by 1st reducer
	2. reads the line number as key and (MovieName,count) as value
	3. emit(one, value)
5. class SortReducer extends Reducer<Text, Text, Text, Text>
	1. reads key and values from the previous mapper
	2. iterating over the value
	3. 	gets count and MovieName by splitting the value
	4. 	storing the count as key and the MovieName as value in a treemap
	5. treemap stores the sorted list of movies by count
	6. For each entry until topn
	7. 	emit(#ratings, MovieName)

## Algorithm for insight 6:
1. class UserMapper extends Mapper<Object, Text, Text, Text>
	1. takes the ratings.dat file as input value
	2. gets the UserID by splitting the data on "::"
	3. emit(UserIDID, one)

2. class UserAgeMapper extends Mapper<Object, Text, Text, Text>
	1. takes the users.dat file as input value
	2. gets the UserID, Age and genre by splitting the rows on "::"
	3. emit(UserID, Age)
3. class MovieReducer extends Reducer<Text, Text, Text, Text>:
	1. gets input from both UserMapper and UserAgeMapper Class
	2. iterating over the values:
	3.	if value is one
	4.		count++
	5.	else
	6.		Age = value
	7. 	emit(Age,count)
4. class AgeSort extends Mapper<LongWritable, Text, Text, Text> 
	1. reads the intermediate output generated by 1st reducer
	2. reads the line number as key and (Age,count) as value
	3. emit(Age, count)
5. class SortReducer extends Reducer<Text, Text, Text, Text>
	1. reads key and values from the previous mapper
	2. iterating over the value
	3. 	add the values together to get total ratings
	4. emit(Age, #ratings)
