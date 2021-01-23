CREATE TABLE Casas (
	id serial PRIMARY KEY,
	house_id integer ,
	city_name varchar(255) , 
	rooms integer , 
	cost integer ,
	tweet_id bigint NOT NULL UNIQUE
);