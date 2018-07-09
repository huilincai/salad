
# Problem 1 - Working with RDDs (5 points)

This is an interactive PySpark session. Remember that when you open this notebook the `SparkContext` and `SparkSession` are already created, and they are in the `sc` and `spark` variables, respectively. You can run the following two cells to make sure that the Kernel is active.

**Do not insert any additional cells than the ones that are provided.**


```python
sc
```





        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://ip-172-31-75-221.ec2.internal:4040">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.2.1</code></dd>
              <dt>Master</dt>
                <dd><code>yarn</code></dd>
              <dt>AppName</dt>
                <dd><code>PySparkShell</code></dd>
            </dl>
        </div>
        




```python
spark
```





            <div>
                <p><b>SparkSession - hive</b></p>
                
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://ip-172-31-75-221.ec2.internal:4040">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.2.1</code></dd>
              <dt>Master</dt>
                <dd><code>yarn</code></dd>
              <dt>AppName</dt>
                <dd><code>PySparkShell</code></dd>
            </dl>
        </div>
        
            </div>
        



In the following cell, make an RDD called `top1m` that contains the contents of the file `top-1m.csv` that you placed into the cluster's HDFS.


```python
top1m = sc.textFile("top-1m.csv")
```

There is one element in the RDD for each line in the file. The `.count()` method will compute how many lines are in the file. In the following cell, type the expression to count the lines in the `top1m` RDD. Run the cell and see the result.


```python
top1m.count()
```




    1000000



## Count the `.com` domains

How many of the websites in this RDD are in the .com domain?

In the following cell, write a code snippet that finds the records with `.com` and counts them. (Hint: use a regular expression.)


```python
def hascom( s ):
    return ".com" in s
```


```python
a = top1m.filter(hascom)
```


```python
a.count()
```




    537633




```python
a.take(10)
```




    [u'1,google.com',
     u'2,youtube.com',
     u'3,facebook.com',
     u'4,baidu.com',
     u'6,yahoo.com',
     u'7,qq.com',
     u'8,amazon.com',
     u'9,taobao.com',
     u'10,twitter.com',
     u'12,tmall.com']



## Histogram the Top Level Domains (TLDs)

What is the distribution of TLDs in the top 1 million websites? We can quickly compute this using the RDD function `countByValue()`.

In the following cell, write a function called `tld` (in Python) that takes a domain name string and outputs the top-level domain.


```python
import re
```


```python
def tld(line):
    line = line.split('.')
    line= line[-1]
    return "{}".format(line)
    

```

In the following cell, map the `top1m` RDD using `tld` into a new RDD called `tlds`. 


```python
tlds=top1m.map( lambda line:tld(line))
```

In the following two cells, evaluate `top1m.first()` and  `tlds.first()` to see if the first line of `top1m` transformed by `tld` is properly represented as the first line of `tlds`. 


```python
top1m.first()
```




    u'1,google.com'




```python
tlds.first()
```




    'com'



Look at the first 50 elements of `top1m` by evaluating `top1m.take(50)`.


```python
top1m.take(50)
```




    [u'1,google.com',
     u'2,youtube.com',
     u'3,facebook.com',
     u'4,baidu.com',
     u'5,wikipedia.org',
     u'6,yahoo.com',
     u'7,qq.com',
     u'8,amazon.com',
     u'9,taobao.com',
     u'10,twitter.com',
     u'11,google.co.in',
     u'12,tmall.com',
     u'13,instagram.com',
     u'14,live.com',
     u'15,vk.com',
     u'16,sohu.com',
     u'17,jd.com',
     u'18,sina.com.cn',
     u'19,reddit.com',
     u'20,weibo.com',
     u'21,google.co.jp',
     u'22,yandex.ru',
     u'23,360.cn',
     u'24,blogspot.com',
     u'25,login.tmall.com',
     u'26,linkedin.com',
     u'27,pornhub.com',
     u'28,google.ru',
     u'29,netflix.com',
     u'30,google.com.br',
     u'31,google.com.hk',
     u'32,google.co.uk',
     u'33,bongacams.com',
     u'34,yahoo.co.jp',
     u'35,google.fr',
     u'36,csdn.net',
     u'37,t.co',
     u'38,google.de',
     u'39,ebay.com',
     u'40,microsoft.com',
     u'41,alipay.com',
     u'42,office.com',
     u'43,twitch.tv',
     u'44,msn.com',
     u'45,bing.com',
     u'46,xvideos.com',
     u'47,microsoftonline.com',
     u'48,mail.ru',
     u'49,pages.tmall.com',
     u'50,ok.ru']



Try the same thing with the `tlds` RDD to make sure that the first 50 lines were properly transformed.



```python
tlds.take(50)
```




    ['com',
     'com',
     'com',
     'com',
     'org',
     'com',
     'com',
     'com',
     'com',
     'com',
     'in',
     'com',
     'com',
     'com',
     'com',
     'com',
     'com',
     'cn',
     'com',
     'com',
     'jp',
     'ru',
     'cn',
     'com',
     'com',
     'com',
     'com',
     'ru',
     'com',
     'br',
     'hk',
     'uk',
     'com',
     'jp',
     'fr',
     'net',
     'co',
     'de',
     'com',
     'com',
     'com',
     'com',
     'tv',
     'com',
     'com',
     'com',
     'com',
     'ru',
     'com',
     'ru']



At this point, `tlds.countByValue()` would give us a list of each TLD and the number of times that it appears in the top1m file. Note that this function returns the results as a `defaultDict` in the Python environemnt, not as an RDD. But we want it reverse sorted by count. To do this, we can set a variable called `tlds_and_counts` equal to `tlds.countByValue()` and then reverse the order, sort, and take the top 50, like this:

```
tlds_and_counts = tlds.countByValue()
counts_and_tlds = [(count,domain) for (domain,count) in tlds_and_counts.items()]
```

In the following cell, run the code above to produce the Python Dictionary.


```python
tlds_and_counts = tlds.countByValue()
```


```python
counts_and_tlds = [(count,domain) for (domain,count) in tlds_and_counts.items()]
```

In the following cell, reverse sort `counts_and_tlds` and display the first 50.


```python
counts_and_tlds.sort(reverse=True)
```


```python
counts_and_tlds[0:50]
```




    [(484593, 'com'),
     (45610, 'org'),
     (41336, 'net'),
     (40239, 'ru'),
     (34374, 'de'),
     (28186, 'br'),
     (18616, 'uk'),
     (16903, 'pl'),
     (15507, 'ir'),
     (12239, 'it'),
     (12041, 'in'),
     (10346, 'fr'),
     (9411, 'au'),
     (8753, 'jp'),
     (8414, 'info'),
     (8070, 'cz'),
     (6518, 'es'),
     (6340, 'nl'),
     (6262, 'ua'),
     (6086, 'co'),
     (5706, 'cn'),
     (5634, 'ca'),
     (5596, 'io'),
     (5246, 'tw'),
     (5009, 'eu'),
     (4812, 'kr'),
     (4794, 'gr'),
     (4788, 'ch'),
     (4512, 'mx'),
     (3841, 'ro'),
     (3836, 'se'),
     (3631, 'no'),
     (3608, 'at'),
     (3484, 'me'),
     (3469, 'tv'),
     (3392, 'be'),
     (3267, 'za'),
     (3266, 'hu'),
     (3076, 'vn'),
     (3039, 'sk'),
     (3020, 'us'),
     (3013, 'ar'),
     (2798, 'edu'),
     (2769, 'dk'),
     (2553, 'tr'),
     (2439, 'pt'),
     (2300, 'biz'),
     (2256, 'cl'),
     (2228, 'id'),
     (2154, 'fi')]



**Question:** `top1m.collect()[0:50]` and `top1m.take(50)` produce the same result. Which one is more efficient and why? Put your answer in the cell below.

### top1m.take(50) is more efficient, because collect() fetches the entire RDD to a single machine.

When you finish this problem, click on the File -> 'Save and Checkpoint' in the menu bar to make sure that the latest version of the workbook file is saved. Also, before you close this notebook and move on, make sure you disconnect your SparkContext, otherwise you will not be able to re-allocate resources. Remember, you will commit the .ipynb file to the repository for submission (in the master node terminal.)


```python
sc.stop()
```
