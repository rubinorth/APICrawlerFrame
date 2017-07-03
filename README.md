# APICrawlerFrame
使用api爬取网站数据的一个程序框架，内有爬取twitter数据的示例

本爬虫程序提供多线程api爬取功能。

**整个程序的框架流程为：**

**1.初始化阶段**

1.1.TaskPool：生成线程池和TaskGenerator对象，以及两个数据队列readDataQueue和writeDataQueue

1.2.TaskGenerator：读入配置文件，并创建读取数据的线程（就一个），即DataQueueTask对象，并交由TaskPool运行

1.3.DataQueueTask：从数据库中读取数据写入readDataQueue，进入等待writeDataQueue的状态

**2.运行阶段**

2.1.TaskPool：循环向TaskGenerator询问是否有新的任务（线程）

2.2.TaskGenerator：不断创建新的GeneralTask对象（新的线程），并交由TaskPool运行

2.3.GeneralTask：从readDataQueue中读取数据，访问api，然后将爬到的数据写入writeDataQueue中

2.4.DataQueueTask：循环读取writeDataQueue中的数据，将其写入数据库。看情况是否要往readDataQueue写入新的数据

**3.结束阶段**

3.1.若DataQueueTask不往readDataQueue写入新的数据，则一旦readDataQueue为空，整个程序会停止。
    此时还在运行的线程会在运行结束后停止。
    
3.2.否则，一般情况下需要手动停止。或者如果一个小时（爬虫一般以小时为周期）没有新的爬虫生成，则程序停止。

框架图示：

                     ==================== readDataQueue——
                                                         |
     TaskPool线程池    * * * * * * * * *                  |——DataQueueTask负责维护这两个队列
                                                         |
                     ==================== writeDataQueue——

**爬虫扩展一般方法：**

1.在TaskGenerator中添加相应的爬虫线程创建代码。

2.在task包下面创建新的包和文件，包结构参照twiTask。

3.视情况在entity包下面创建实体类。

4.在dataQueueTask包下面创建新的DataQueueTask的子类，实现自己的数据操作方法。

5.在TaskPool中创建TaskGenerator时修改参数。

**twitter api备注：**

1.twitter提供的api种类较多，代码里用的是REST API。

2.streaming api 可以跟踪某个话题或某个用户。

3.爬tweets时可以看看这个页面https://dev.twitter.com/rest/public/timelines
