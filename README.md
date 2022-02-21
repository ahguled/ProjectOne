
<h1>Pokedex Project</h1>


A simple command line interface project used to Query interesting Pokemon facts using Spark and HiveQL via HDFS. User has 6 predefined queries to select from and access to over 900 different pokemons across all generations and games.


<h2>Technologies Used </h2>

* Scala Version 2.11.12
* SBT Version 1.6.0
* Apache Spark–Hive Version 3.1.2
* MySQL Version 8.0.27
* Hadoop 3.0
* VS CODE



<h2>Features</h2>

*  Allows for user log-in with two distinct user types: Basic and Admin which is stored in a MySQL database.
*  Admin has special features such as updating database or changing a user password or deleting a user.
*  Total of 6 different queries to choose from user menu.
*  Users can alter user information such as username or password. 

<h3>To-Do List:</h3>

1. Add more artistic touches via ASCII Art.
2. Edit the menu system while-loops so that users stay logged in after admin operations.

<h2>Getting Started</h2>

- Users must add a base admin to a sql database in order to utilize the program. 
- The table should have columns: name, username, password and user type(BASIC vs ADMIN).
