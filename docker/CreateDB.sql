CREATE TABLE Casas (
	id serial PRIMARY KEY,
	City_ID integer ,
	City_Name varchar(255) , 
	Rooms integer , 
	Cost integer ,
	code integer NOT NULL UNIQUE
);
