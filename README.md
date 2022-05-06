> 这个项目是我在看尚硅谷电信项目时写着玩的

数据的流程是kafka->hbase->spark->mysql

原始日志在kafka，持久化到hbase，spark读取hbase，做聚合分析之后将结果写入到mysql
