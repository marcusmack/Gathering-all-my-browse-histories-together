This post is to demo how to get records from different databases, do some data massages, then consolidate them into one SQL database table using python. So if you want to find something from your web browser history, you will only need to look for it in one place (the consolidated table) instead of opening and searching _all_ the browsers you use/used.

**Disclaimer:** This is the very first original post of my code work. Please bear with me on my writing, wording, and formats, etc.   :sweat_smile:
As well as, this is just a weekend project for fun (even though it took me more than one weekend. :sweat_smile:). 

Let's get started.

### Creating connections and have the SQL queries ready.

First of all, import libraries and create connection to our DB.
``` Python
import pandas as pd, sqlite3
myHistoryDB = sqlite3.connect(r"your DB's path") # this is the DB that contains everything.
```
Also, create connections with browsers' DBs and put them into a dictionary. 
```python
ffconn = sqlite3.connect(r"your browser's DB's path")
vvconn = sqlite3.connect(r"your browser's DB's path")
operaconn = sqlite3.connect(r"your browser's DB's path")
braveconn = sqlite3.connect(r"your browser's DB's path")
browserConns = {1:ffconn, 2:vvconn, 3:operaconn,4:braveconn}
```

Then, create another dictionary called `queries` for the queries we are going to use to pull records from the browsers' DBs. Why 4 browsers but only 2 queries? It is because browser 2, 3, and 4 are all Chromium-based browser. They all use the same method to convert the numeric datetime to normal datetime format. You will see what I mean in the summary table output.
```Python
FFquery = '''
SELECT DATETIME(last_visit_date/1000000,'unixepoch','localtime') as LastVisit,title,url,last_visit_date as last_visit_time
from moz_places
where last_visit_date > {}
order by last_visit_date;
'''

chrquery = '''
SELECT datetime(last_visit_time/1000000-11644473600,'unixepoch','localtime') as LastVisit,title,url,last_visit_time
from urls
where last_visit_time > {}
order by last_visit_time;
'''
queries = {1:FFquery, 2:chrquery , 3:chrquery, 4:chrquery}
```
**Pretty easy right?** 

This post is about adding new records to an existing table. 
Let's assume that table `History` which is the table stores all the records, was created and has records in it already. 
### Interacting with databases.
Let's get the latest history records from table `History` by browsers. 
```Python
query = '''
SELECT BrowserID,Browser,count(*) as totHistories,max(last_visit_time) as latestVisitedTime, max(LastVisit) as LastVisit
from History
INNER join Browsers on History.BrowserID = Browsers.id
group by BrowserID
order by BrowserID desc;
'''
myHisSummary = pd.read_sql(query,myHistoryDB)
myHisSummary
```
Here is the result. 

BrowserID|Browser|totHistories|latestVisitedTime|LastVisit
---|---|---|---|---|
4|Brave|1396|13226044647296850|2020-02-12 21:17:27
3|Opera|2691|13223464426906810|2020-01-14 00:33:46
2|Vivaldi|3590|13224916461501190|2020-01-30 19:54:21
1|FireFox|4248|1580458216689000|2020-01-31 00:10:16

Total records by browsers(`totHistories`) are also pulled because we will need them to verify our result. You might wonder why there are two columns(`latestVisitedTime` and `LastVisit`) for the last visit time. 
* Column `latestVisitedTime` in numeric format is the timestamp that the browsers use. 
* Column `LastVisit` is the timestamp for our reference.

It is my habit to keep a copy of the table we are going to change. So if something goes south, I can undo it easily. But it might consume too much computation power if the table is getting too big.
```Python
allRecords = pd.read_sql(r'select * from History',myHistoryDB)
```
Let's see how the `History` table looks like.

id | LastVisit	| title	| url	| last_visit_time	| BrowserID
---- | ----	| ----	| ----	| ----	| ----
438 |	2019-04-13 20:07:50	| Booking.com: Cheap Hotels |	http://www.booking.com/| 13199684870396256	| 3
443 |2019-06-12 00:14:36|LinkedIn|https://www.linkedin.com/jobs/view/1260337591/...|13204797276999400|3
446|2019-06-12 00:16:23|LinkedIn|https://www.linkedin.com/messaging/thread/6532...|13204797383301360|3
447|2019-06-12 00:16:44|financial data analyst Jobs LinkedIn|https://www.linkedin.com/jobs/search/?f_JT=F&k...|13204797404557600|3
448|2019-06-12 00:17:14|Financial Analyst Google LinkedIn|https://www.linkedin.com/jobs/view/1319232200/...	|13204797434430200|3

**Piece of cake right? Let dive into the fun part.  :muscle:**

Let's create an **empty** dataframe with column names to keep all the new records from different browsers together.
```python
newRecords = pd.DataFrame(columns=['LastVisit','title','url','last_visit_time'])
newRecords
```
```markdown
LastVisit |title|url|last_visit_time
----|----|----|----
```

Then, we can loop through our `myHisSummary` dataframe to get the *new* history records from our browsers. 
```python
for (r0,bid,browser,totCount,lastVisitTime,LastVisit) in myHisSummary.itertuples():
    print ('======',browser,', last vistied time:',lastVisitTime,'which means',LastVisit,'========')
    newRec = pd.read_sql(queries[bid].format(lastVisitTime),browserConns[bid])
# records from the browsers do not have browserID. We need to add it a 'BrowserID' column according to our `History` table.
    newRec['BrowserID'] = bid 
#     print(newRec.tail())
    print ('-----------------------------------------------------------')
    print('\t{} new records: {} since last update'.format(browser, newRec.shape[0]))
    print('------------------------------------------------------------')
    newRecords = newRecords.append(newRec,ignore_index=True,sort=False)
```
Here is the summary output.
```markdown
====== Brave , last vistied time: 13226044647296850 which means 2020-02-12 21:17:27 ========
-----------------------------------------------------------
	Brave new records: 4 since last update
------------------------------------------------------------
====== Opera , last vistied time: 13223464426906810 which means 2020-01-14 00:33:46 ========
-----------------------------------------------------------
	Opera new records: 0 since last update
------------------------------------------------------------
====== Vivaldi , last vistied time: 13224916461501190 which means 2020-01-30 19:54:21 ========
-----------------------------------------------------------
	Vivaldi new records: 567 since last update
------------------------------------------------------------
====== FireFox , last vistied time: 1580458216689000 which means 2020-01-31 00:10:16 ========
-----------------------------------------------------------
	FireFox new records: 445 since last update
------------------------------------------------------------
```
***Bonus Reading:*** Thanks to [Wei Xia](https://medium.com/@xiawei27149)'s this wonderful [article](https://medium.com/swlh/how-to-efficiently-loop-through-pandas-dataframe-660e4660125d) . I realized that using the build-in `itertuples` function is not the best way to loop through the dataframe(DF). But since our `myHisSummary` DF has only 4 rows, it won't be any big difference. So I keep the `.itertuples` method in order to show how it works. :smiley:

Here is how the `newRecords` DF looks like.

LastVisit|title|url|last_visit_time|BrowserID
---|---|---|---|---|
 2020-01-27 01:48:12|Rockfile - Solid as a Rock|https://rockfile.co/f/tokqvvlaz-uxchu3maesfog|1580118492972000|1.0
 2020-01-27 00:25:37|Love valley in Goreme village, Turkey. Rural l...|https://clkde.tradedoubler.com/click?p=264311&...|13224587137708428|2.0
 2020-01-27 00:26:01|Pan Timelapse View Of Goreme Village In Cappad...|https://www.google.com/url?sa=i&source=images&...|13224587161969714|2.0
 2020-01-27 00:11:13|Google Drive: Sign-in|https://accounts.google.com/ServiceLogin?servi...|13224586273910680|2.0
 2020-01-27 01:09:23|Spotify|https://www.spotify.com/us/premium/?checkout=f...|1580116163095000|1.0
 2020-01-25 03:05:36|Amazon.com: Buying Choices: 3M 8511HB1-C-PS Sa...|https://www.amazon.com/gp/offer-listing/B08469...|13224423936809032|2.0
 2020-01-27 00:32:14|göreme in the cappadocia region of turkey 4k -...|https://www.google.com/search?hl=en&biw=1714&b...|13224587534975438|2.0


In most cases, we do not need to keep all the records of when did we browse that url. We only need to know when we last visited it, which will make our table compact and more efficient also. In order to guarantee this, I added an `unique` constraint to the `History` table's `url` column. It is not supposed to be triggered if our works on the DF are all good, but never say never right? It did trigger a couple of times when I was trying to load the records into the table.  :sweat_smile: 

Thanks to that constraint. I fixed my error before loading them into the table.    :sweat_drops:   :sweat_drops:

Here is what we need to do to avoid triggering the unique constraint.
```python
# sort the newrecords by LastVisit first.
newRecords.sort_values('LastVisit',inplace=True)
# remove duplicates
newRecordswoDup = newRecords.drop_duplicates('url',keep='last')
```

No more duplicates in the DF now. Then we should decide which record needs to be inserted or updated according to whether the url is already in the `History` table. We can do that by adding a new column called `newUrl` and set the default value to `0` which means the record is **not** new url. 
Then we can select the urls that are not in the `Hisotry` table and assign value `1` to them.
```python
newRecordswoDup['newUrl'] = 0
# find out it is new record or not.
newRecordswoDup.loc[~newRecordswoDup.url.isin(allRecords.url),'newUrl']=1
```
Ha. The `allRecords` DF is not just for backup purpose only. :sunglasses:

Let's check how many records need to be updated and inserted respectively.
```python
print('**** {} records should be updated. ****'.format(newRecordswoDup.url[newRecordswoDup.newUrl==0].size))
print('**** {} records should be inserted. ****'.format(newRecordswoDup.url[newRecordswoDup.newUrl==1].size))
```
```
**** 155 records should be updated. ****
**** 841 records should be inserted. ****
```

**Finanlly**, here we are! We have everything ready!  :tada:  :tada:  :tada:

Let's go ahead and **update** the existing urls first. 
```sql
updateQuery = '''
update History
set LastVisit = ?,
    title = ?,
    last_visit_time = ?,
    BrowserID = ?
where url = ?
'''
```
```python
needUpdateUrls = newRecordswoDup.loc[newRecordswoDup.newUrl==0,['LastVisit', 'title', 'last_visit_time', 'BrowserID','url']].values
```
Wondering why I use the `.values` above? Please refer to the ***Bonus Reading*** part of this post.  :sunglasses:

Take a deep breath before running the codes below because all our works above will not change our DB's table, but these will.  :eyes:
```python
cursor = myHistoryDB.cursor()
cursor.executemany(updateQuery,needUpdateUrls)
```
The update query just ran through without a notice? That is it?! Did I mess up my precious `History` table?? -   :scream_cat: -   :scream_cat:
Don't worry. Those updates have not been pass/commit to the DB yet.
Let check whether it worked before we actually updating the table.
```
cursor.execute(query).fetchall()
```
Here is the output.
```
[(4, 'Brave', 1392, 13226127757503206, '2020-02-13 20:22:37'),
 (3, 'Opera', 2681, 13223464426906810, '2020-01-14 00:33:46'),
 (2, 'Vivaldi', 3590, 13226125535957899, '2020-02-13 19:45:35'),
 (1, 'FireFox', 4262, 1581657153485000, '2020-02-13 21:12:33')]
```
The `LastVisit` column of the summary result changed from our previous result.

BrowserID|Browser|totHistories|latestVisitedTime|LastVisit
---|---|---|---|---|
4|Brave|1396|13226044647296850|2020-02-12 21:17:27
3|Opera|2691|13223464426906810|2020-01-14 00:33:46
2|Vivaldi|3590|13224916461501190|2020-01-30 19:54:21
1|FireFox|4248|1580458216689000|2020-01-31 00:10:16

Great! We can commit our update query's work to the DB now.
```python
myHistoryDB.commit()
```

**Let's check whether it worked from DB** by checking what is the current summary of the DB.
```python
pd.read_sql(query,myHistoryDB)
```

BrowserID|Browser|totHistories|latestVisitedTime|LastVisit
---|---|---|---|---|
4|Brave|1392|13226127757503206|2020-02-13 20:22:37
3|Opera|2681|13223464426906810|2020-01-14 00:33:46
2|Vivaldi|3590|13226125535957899|2020-02-13 19:45:35
1|FireFox|4262|1581657153485000|2020-02-13 21:12:33

Worked as expected. -   :fireworks: -   :fireworks:

Are we done? Not yet! 

Do not forget those **new** urls. It is easy though. Just one line of code will work perfectly.
```python
newRecordswoDup.loc[(newRecordswoDup.newUrl==1),allRecords.columns[1:]].to_sql('History',myHistoryDB,if_exists='append',index=False)
```

I like to add an `assert` to reassure myself. So if the original count of the urls *plus* the total of the new urls does not equal to the current total url count of the `History` table, it will raise an error, which means something wrong. We need to dig into the `History` table to find out. 
```python
assert myHisSummary.totHistories.sum() + (newRecordswoDup.newUrl==1).sum() ==pd.read_sql(query,myHistoryDB).totHistories.sum(), 'DB record count != new added'
```
If no error, our `History` table is up to date! 
### :checkered_flag:   Job done!!   :checkered_flag:

### Thoughts.   :thought_balloon:  :thought_balloon:
Not everyone uses multiple browsers and eager to keep and consolidate those browse histories like me, but those concepts and codes here can be applied to any other consolidation scenarios such as consolidating excel/csv format bank statements into one table. I actually did that for my company, which boosted my efficiency a lot.   :v:
Another reason I chose databases instead of excel/csv to consolidate except for I need that `History` table for myself is I saw plenty of tutorials or posts regarding using python to interact with excel/csv and database, but not too much for the interactions across databases. And browse's databases are the most convenient real-world databases you can get easily from your computer.

That is it. Here is my first original post.

Thanks for your time reading my first post. Hope you enjoyed it.
