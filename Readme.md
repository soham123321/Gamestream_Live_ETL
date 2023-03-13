## PART 1: The environment:

The environment is a `docker-compose.yaml` file that simulates a distributed environment. It consists of the following services:

Databases: 
- **mssql** - A Microsoft SQL Server database that stores the player and team reference data. The database is called `sidearmdb` and the tables are `players` and `teams`.
- **minio** - An S3 compatible object store that contains the live game stream. The game stream is stored in the `minio/gamestreams` bucket.
- **mongodb** - A mongodb database that stores the game stream's real-time box score so the web developers can create a page from the data. The box score is written to the `mongodb/sidearm/boxscores` collection.

Tools:
- **drill** - An instance of Apache Drill that can be used to query the databases. The `drill-storage-plugins` folder contains the configuration files for the databases. You will need to modify these with specifics for them to work.
- **jupyter** - An instance of Jupyter Lab that can be used to write PySpark code. The `work` folder contains the `Start.ipynb` that demonstrates the base spark configuration. 

Scripts:
- **gamestream** - A python script that simulates a lacrosse game stream. It writes the game stream to a file in the `minio/gamestreams` S3 bucket.

## PART 2: Managing the environment

### A warning about other containers

NOTE:  If you have `advanced-database` containers running, ***this will cause problems with the midterm***. You should stop those containers before starting the midterm environment.

To **check if there are containers running**, run the following command:   
```PS> docker ps```

To **stop all running containers**, run the following command:   
```PS> docker ps -q | % { docker stop $_ } ```

### Starting and stopping the environment

Free of other containers running you can start the environment of the midterm exam:

To **start the environment**, run the following commands:  

1. Start the databases:  
 ```PS> docker-compose up -d mssql mongodb minio```
2. Make sure the databases are running  
```PS> docker-compose ps```   
(you should see `mssql`, `mongodb`, and `minio` all running)
3. Start the tools:   
```PS> docker-compose up -d drill jupyter```
4. Make sure the tools are running  
```PS> docker-compose ps```
5. Finally, start the gamestream:   
```PS> docker-compose up -d gamestream```
4. Make sure the gamestream is running by looking at the logs:  
```PS> docker-compose logs gamestream```   
(you should see `gamestream` running, outputting game events. This will take time so you might need to run this command a few times to check progress.)
5. Valid `gamesteam` output looks like this:
```
  | Added `s3` successfully.
  | Bucket created successfully `s3/gamestreams`.
  | Bucket created successfully `s3/boxscores`.
  | Commands completed successfully.
  | Commands completed successfully.
  | INFO:root:Waiting for services...
  | INFO:root:Bucket exists...ok
  | INFO:root:Starting Game Data Stream. Delay: 1 second == 0.25 seconds.
  | INFO:root:Wrote gamestream.txt to bucket gamestreams at 59:51
```
IF YOU DON'T SEE THIS, A SERVICE IS NOT RUNNING.


To **stop the environment**, run the following command:  
```PS> docker-compose down```

If you need to **start over from the very beginning** (erase the volumes) run the following command:  
```PS> docker-compose down -v```


### Managing the game stream
The `gamestream` container simulates the live game. Each time the game stream is started:
- The `players` and `teams` database tables are reset back to their original state.
- The live game is replayed from the beginning, writing events to `s3/gamestreams/gamestream.txt` as they occur.
- Restarting the game steam will NOT erase any other data in `mongo` or other tables in `mssql`. See "Start over from the very beginning" if you need to do that.

To **Restart the game stream:**, run the following command:  
 ```PS> docker-compose restart gamestream```

To **view the gamestream activity**, run the following command:  
 ```PS> docker-compose logs gamestream```

#### Adjusting the gamestream speed

By default the game stream "plays" at 4x speed. That 0.25 seconds of real time is 1 second of game time. You can adjust this by setting the `DELAY` environment variable in the `.env` file. `DELAY=1` plays the game in real time, and `DELAY=0.1` plays the game at 10x speed. If you adust the `DELAY` environment variable, you will need to rebuild the `gamestream` container. To do this, run the following commands:  
```PS> docker-compose stop gamestream```  
```PS> docker-compose rm -force gamestream```  
```PS> docker-compose up -d gamestream```   


# PART 3: The Problem

The objective is to create a data pipeline that processes a simplified, in game stream from a simulated a lacrosse game. The game stream has been simplified to only process goals scored. There are two parts to this problem:

1. At any point while the game is in progress, the game stream should be converted into a JSON format so the web developers can use it to create a box score page. This JSON should be written to the `mongodb/sidearm/boxscores` collection, and should contain all the data necessary to display the box score page from a single query to the database.
2. When the game is over, the player and team reference should be updated to reflect the team records and player statistics. Normally you would update the `mssql` tables, but for this exam you will create new tables with the updated data, `players2` and `teams2` respectively. This is mostly because spark does not support row-level updates. In a real world scenario, you would write an SQL script on `mssql` to update the tables. from the changes in the `players2` and `teams2` tables, but that is outside the scope of this exam. 

## Game Stream 

While the game is going on, there is a file called `gamestream.txt` located in the  `minio/gamestreams` S3 bucket. Each time an in-game event happens, the event is  appended to this file.
To simplify things, the game stream only reports shots on goal. Here is the format of the file each line is an event and the fields are separated by a space:

```
0 59:51 101 2 0
1 57:06 101 6 0
2 56:13 205 8 1
3 55:25 101 4 0
```

### Data Dictionary for gamestream.txt
- The first column is the event ID. These are sequential. An event ID of -1 means the game is over.
- The second column is the timestamp of the event in the format `mm:ss`. This counts down to 00:00. For example the first event occurred 9 seconds into the game.
- The third column is the team ID, indicating team took the shot on goal. In the simulation there are only two teams, `101` and `205`.
- the fourth colum is the jersey number of the player who took the shot.
- the final column is a `1` if the shot was a goal, `0` if it was a miss.

## Player and Team Reference Data

The player and team reference data is stored in a Microsoft SQL Server database.  The database is called `sidearmdb` . The database has two tables, `players` and `teams` with the following schemas, respectively:

```sql
CREATE TABLE teams (
    id int primary key NOT NULL,
    name VARCHAR(50) NOT NULL,
    conference VARCHAR(50) NOT NULL,
    wins INT NOT NULL,
    losses INT NOT NULL,
)

CREATE TABLE players (
    id int  primary key NOT NULL,
    name VARCHAR(50) NOT NULL,
    number varchar(3) NOT NULL,
    shots INT NOT NULL,
    goals INT NOT NULL,
    teamid INT foreign key references teams(id) NOT NULL,
)
```

The `teams` table, only has two teams, `101 = syracuse` and `205 = johns hopkins`.  Each team has a conference affiliation, and a current win / loss record.

The `players` table has 10 players for each team. Each player has a name, jersey number, shots taken, goals scored, along with their team id.


### PART 3.1: The game stream's real-time box score

Each time you run your code while the game is ongoing, you should write a new `boxscore` document to the `mongodb/sidearm/boxscores` collection. That way sidearm web developers can read the latest document 's contents to render a webpage for live box score stats while the game is going on.

For simplicity, assume team `101` is the home team and team `205` is the away team.  

The document should have the following structure (consider this an example)

```json
{
    "_id" : "UseTheEventIDFrom gamestream.txt",
    "timestamp" : "55:25",
    "home": {
        "teamid" : 105,
        "conference" : "ACC",
        "wins" : 5,
        "losses" : 2,
        "score" : 3,
        "status" : "winning",
        "players": [
            {"id": 1, "name" : "sam",  "shots" : 3, "goals" : 1, "pct" : 0.33 },
            {"id": 2, "name" : "sarah",  "shots" : 0, "goals" : 0, "pct" : 0.00 },
            {"id": 3, "name" : "steve",  "shots" : 1, "goals" : 1, "pct" : 1.00 },
            ...
        ]
    },
    "away": { ... }
}
```

NOTES:

- `"status"` should be `"winning", "losing" or "tied"` based on the current `home.score` and `away.score`
- the `"_id"` should be the latest event ID from the game stream, at the time the box score was written. NOTE: This is atypical, but simplifies the problem in this case.
- the "timestamp" should be the timestamp from the game stream.
- Every player on the roster (in the players table) should appear in the box score.
- The stats in the box score should be the current stats for the player in game only, and not include the stats in the `players` table. So you will need to add up the shot and goal for every player at that point in the game stream.
- Calculate the `pct` field so the web developers don't have to do this!
- No need to figure out how to schedule your box score code. Just run it at least 5 times during the game stream, so there are multiple documents in the collection.
- game is over when the clock hits 00:00. 

### PART 3.2: Updating stats in the database when the game is over

After the game is complete, the tables in the `mssql` `sidearmdb` database should be updated, based on the final box score. Specifically:
- update the win/loss record for each team in the `teams` table
- update the shots and goals for each player in the `players` table

NOTES:

- We will not update the actual tables. Instead we will create new tables called `teams2` and `players2` with the updated data. It's anti-big data to perform row-level updates. The proper way to move the updates into the original tables would be to write an MSSQL script to update the tables, but that is outside the scope of this exam.

## Questions

1. Write a drill SQL query to list the team and player data. Specifically display team name, team wins, team losses player name, player shots and player goals. 

2. Write a drill SQL query to display the gamestream. Label each of the columns in the gamestream with their appropriate columns names from the data dictionary.

3. Write pyspark code (in SQL or DataFrame API) to display the gamestream. Label each of the columns in the gamestream with their appropriate columns names from the data dictionary.

4. Write pyspark code (in SQL or DataFrame API) to group the gamestream by team/player jersey number adding up the shots and goals. Specifically:
    - Values dependent on team and player: total shots and goals for each player.
    - Value dependent on only team: total goals (this should repeat for every row with the same team id)
    - The last event id and timestamp for that point in time in the game  (every row should have the same event ID and as timestamp as these represent the point in time when the stats were compiled)

    For example (sample - not the actual data):

    | event_id | timestamp | team_id | jersey_number | shots | goals | team_goals |
    |----------|-----------|---------|---------------|-------|-------|------------|
    | 45       | 22:34     | 105     | 1             | 1     | 1     | 2          |
    | 45       | 22:34     | 105     | 2             | 2     | 0     | 2          |
    | 45       | 22:34     | 105     | 3             | 5     | 1     | 2          |
    | 45       | 22:34     | 201     | 1             | 7     | 1     | 1          |
    | 45       | 22:34     | 201     | 99            | 3     | 0     | 1          |
    

5. Write pyspark code (in SQL or DataFrame API) to join the output from question 4 with the player and team reference data `mssql` so that you have the data necessary for the box score. 

6. Write pyspark code (in SQL or DataFrame API) to transform the output from question 5 into the box score document structure shown in part 3.1.

7. Write pyspark code (in SQL or DataFrame API) to write the box score completed in question 6  to the `mongo.sidearm.boxscores` collection.

8. Combine parts 4-7 into a single pyspark script that will run the entire process of creating the box score document. Make sure to run this a couple of times while the game stream is going on.

9. Write a drill SQL query to display all the box scores.

10. Write a drill SQL query to display the latest box score.

11. When the game is complete, write pyspark code (in SQL or DataFrame API) update the `wins` and `losses` for the teams in the `teams` table. Specifically, load the `teams` table and update it, then display the updated data frame.

12. Write pyspark code (in SQL or DataFrame API) to write the updated in question 11 to a new `mssql.sidearmdb.teams2` table.

13. When the game is complete, write pyspark code (in SQL or DataFrame API) update the `shots` and `goals` for the players in the `players` table. Specifically, load the `players` table and update it, then display the updated data frame.

14. Write pyspark code (in SQL or DataFrame API) to write the updated in question 11 to a new `mssql.sidearmdb.players` table.

15. Re-write drill SQL query from question 1 to use the updated `players2` and `teams2` tables.
