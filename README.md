## zorathos-data-receiver

zorathos-data-receiver是中台的数据接收器，基于zorathos-data-model构建

data-receiver-human-machine是翔腾项目的数据接收器module，代码结构如下

```bash
.
└── org
    └── datacenter
        ├── agent # 数据爬虫
        │         ├── personnel # 人员数据爬虫 允许独立启动导出JSON
        │         ├── plan # 飞行计划爬虫 允许独立启动导出JSON
        │         ├── sorties # 架次数据爬虫
        │         └── util # 爬虫工具
        └── receiver # 数据接收器
            ├── crew # 人员数据接收器
            │          └── util
            ├── equipment # 装备数据接收器
            ├── physiological # 生理数据接收器 未完成
            │          └── base
            ├── plan # 飞行计划接收器
            │          └── util
            ├── real # 实装数据接收器
            ├── simulation # 仿真数据接收器
            │          └── base
            ├── sorties # 架次数据接收器
            └── util # 接收器工具
```

### 运行指南
TBD

### contributing
TBD


```shell
--add-exports=java.base/sun.net.util=ALL-UNNAMED 
--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED 
--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED 
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED 
--add-exports=jdk.compiler/com.sun.tools.javac.pa.javac.tree=ALL-UNNAMED
--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED 
--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED 
--add-opens=java.base/java.lang=ALL-UNNAMED 
--add-opens=java.base/java.net=ALL-UNNAMED 
--add-opens=java.base/java.io=ALL-UNNAMED 
--add-opens=java.base/java.nio=ALL-UNNAMED 
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED 
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED 
--add-opens=java.base/java.text=ALL-UNNAMED 
--add-opens=java.base/java.time=ALL-UNNAMED 
--add-opens=java.base/java.util=ALL-UNNAMED 
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED 
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED 
--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
```
