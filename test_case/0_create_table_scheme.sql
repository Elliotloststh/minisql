create table City(
ID int,
Name char(35) unique,
CountryCode char(3),
District char(20),
Population int,
primary key(ID)
);

create table CountryLanguage(
CountryCode char(3),
Language char(30),
IsOfficial char(1),
Percentage float,
primary key(CountryCode)
);
