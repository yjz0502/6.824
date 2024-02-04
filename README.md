#### First Commit 
实现Coordinator响应Worker的RPC请求分配Map任务，

Worker调用Mapf函数将输出键值对写入到中间文件中

#### Second Commit
7个测试通过5个，Worker异常推出或超时没有处理

#### Third Commit
实现c.Done方法

#### Third Commit
SetMap和SetReduce回复结构体增加字段AllComplete检测是否所有任务都完成

所有map任务状态为Complete才不检测，

所有Reduce任务状态为Complete才退出Worker

7个测试通过6个，crash test失败

TODO：

TaskStatus新增字段：对应的WorkerID

Master每十秒监控Worker是否存活