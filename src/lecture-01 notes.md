第一个lab: lab-mr
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

目标是简单实现一个map-reduce框架。

大体方案是：构建coordinator和worker，分别启动，然后通过RPC通信，
worker通过加载plugin的方式，加载调用词频统计的业务代码(wc.go编译成wc.so)。

课程源码提供了骨架结构，你只需要往里面添加主要逻辑。

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










