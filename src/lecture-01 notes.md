第一个lab: lab-mr
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

目标是简单实现一个map-reduce框架。

大体方案是：构建coordinator和worker，分别启动，然后通过RPC通信，
worker通过加载plugin的方式，加载调用词频统计的业务代码(wc.go编译成wc.so)。

课程源码提供了骨架结构，你只需要往里面添加主要逻辑。

把你的实现写在这3个文件中 mr/coordinator.go, mr/worker.go, mr/rpc.go.

课程源码还提供了一个顺序处理的版本: mrsequential.go，
输入是文件，输出文件mr-out-0， 内容是这些文件的词频统计，
你可以拿 mrsequential.go 的代码做参考，
写出worker来，并用前者的结果来和后者做个验证。

课程源码还提供了一个test-mr.sh作为测试脚本，校验业务代码wc和index MapReduce生成
的结果是否正确，同时也会检查并行实现和崩溃恢复特性。

在做这个lab的时候，脑海中要有一幅map-reduce的协作图。
有coordinator驱动的，也有worker驱动的。
要知道map和reduce都是worker来做的，coordinator 只负责任务的记录和分发。
worker如何支持多种任务呢？那得根据coordinator的标记的任务类型来做方法分派。

按照课程源码的设置，应该是 coordinator 作为server 只记录任务（被动分发），
worker 作为client主动拉取任务，将结果写入文件中，然后RPC通知给 coordinator。
coordinator有个Done()方法来检查整个任务是否执行完成，客户端代码会for循环Done()等待任务结束。

1、每个文件对应一个map任务编号， map任务将一个文件的内容分成单词数组，然后按 Y = ihash(key) % NReduce 映射到对应的reduce任务。
中间产生的文件建议命名为 mr-X-Y, 其中X为map任务编号, Y为reduce任务编号.
2、每个reduce任务处理对应 mr-*-Y的文件。

问：这里为什么要做hash？
答：保证分布在不同文件中相同的单词被汇入相同文件中。

你可以加载运行 crash.go 来验证崩溃恢复能力。

RPC调用参考官网文档： https://pkg.go.dev/net/rpc


# 命令行操作

1. 将word counter程序编译成一个插件
```shell
$ go build -buildmode=plugin ../mrapps/wc.go
```

2. 进入main文件夹，启动协调器 coordinator
```shell
$ rm mr-out*
$ go run mrcoordinator.go pg-*.txt
```
这里pg-*.txt arguments就是输入的文件名列表; 
每个文件对应一个切分 "split", 一个文件对应到一个Map任务.

3. 打开其他命令行窗口，启动多个worker：
```shell
$ go run mrworker.go wc.so
```
此时work应该从coordinator拉取任务并执行，任务完成后，查看文件 mr-out-* 的输出结果。 

4. 最后cat+sort把所有输出结果合并、排序，应该和sequential的输出一致，就像这样：
```shell
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```

5. 测试脚本
课程里面提供了测试脚本(main/test-mr.sh)，它会校验wc 和 indexer MapReduce 应用程序的输出是否正确。
这个脚本还会校验你的MapReduce任务是否实现了并行处理、是否支持奔溃恢复。

如果你现在就执行这个脚本，它会挂住不动，因为 coordinator 没有停：
```shell
$ cd src/main
$ bash test-mr.sh
*** Starting wc test.
```

你可以把 mr/coordinator.go的 Done() 函数写死返回 ret := true, 这样coordinator马上就能退出，从而能看到我们的测试脚本提示错误。
```shell
$ bash test-mr.sh
*** Starting wc test.
sort: No such file or directory
cmp: EOF on mr-wc-all
--- wc output is not the same as mr-correct-wc.txt
--- wc test: FAIL
$
```

你可能会遇到一些Go的RPC问题，比如：
```shell
2019/12/16 13:27:09 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```
将coordinator注册为RPC server的时候，会校验 coordinator的所有方法是否适用于RPC（拥有有3个参数），
我们知道这里Done函数不是从RPC调用的，所以请忽略这些问题。



# 遇到的问题
1. 用goland debug 启动worker时，提示插件版本号不一致：

2023/03/30 18:27:46 cannot load plugin /Users/john.zhang/GolandProjects/distributed-systems/src/main/wc.so, 
error:plugin.Open("/Users/john.zhang/GolandProjects/distributed-systems/src/main/wc"): 
plugin was built with a different version of package runtime/internal/atomic Exiting.

搜索github也没找到合适的办法：https://github.com/golang/go/issues/27751

命令行直接run可以跑起来.





