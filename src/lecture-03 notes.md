GFS 设计

GFS 要解决的问题：
- 超大文件尺寸：分片sharding
- 超快文件读写：并行处理
- 错误自动恢复：多副本
- 多副本一致性问题：共识算法


GFS 总体设计：
- Client，Master，Chunk Server 架构


