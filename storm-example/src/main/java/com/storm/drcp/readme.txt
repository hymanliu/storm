DRPC

1.配置DRPC service地址
vi storm.yaml
drpc.servers:
    - "hyman1"
    - "hyman2"

2.分别在hyman1,hyman2上启动DRPC
cd $STORM_HOME
storm drpc >> ../logs/drpc.log 2>&1 &



storm jar /opt/share/jar/storm-example.jar com.storm.drcp.BasicDRPCTopology drpc-demo
