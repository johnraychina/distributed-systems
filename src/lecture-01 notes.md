第一个lab: lab-mr
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

目标是简单实现一个map-reduce框架。

方案是，构建coordinator和worker，分别启动，然后通过RPC通信，
worker通过加载plugin的方式，加载调用词频统计的业务代码(wc.go编译成wc.so)。

课程源码提供了骨架结构，你只需要往里面添加主要逻辑。

课程源码还提供了一个顺序处理的版本: mrsequential.go，
输入是文件，输出文件mr-out-0， 内容是这些文件的词频统计，
你可以拿 mrsequential.go 的代码做参考，
写出worker来，并用前者的结果来和后者做个验证。

课程源码还提供了一个test-mr.sh作为测试脚本，校验业务代码wc和index MapReduce生成
的结果是否正确，同时也会检查并行实现和崩溃恢复特性。














