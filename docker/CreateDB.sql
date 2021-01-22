CREATE TABLE Tweets (
    id serial PRIMARY KEY,
    Tweet_ID integer ,
    Person_name varchar(255) ,
    Years integer ,
    Salary integer ,
    Members integer ,
    Hobby1 varchar(255) ,
    Score_Hobby1 integer ,
    Hobby2 varchar(255) ,
    Score_Hobby2 integer ,
    Hobby3 varchar(255) ,
    Score_Hobby3 integer ,
    Hobby4 varchar(255) ,
    Score_Hobby4 integer 
);

CREATE TABLE Casas (
	id serial PRIMARY KEY,
	City_ID integer ,
	City_Name varchar(255) , 
	Rooms integer , 
	Cost integer ,
	code integer UNIC KEY
);
