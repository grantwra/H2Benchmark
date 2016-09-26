
adb -s 042b0f1109bbdb33 reboot
sleep 30s
adb -s 042b0f1109bbdb33 root
sleep 2m
i=1

for j in `seq 1 1`;
do
	adb -s 042b0f1109bbdb33 shell sh /data/removeBenchmarkData.sh
	adb -s 042b0f1109bbdb33 shell sh /data/preBenchmark.sh #create database 
	adb -s 042b0f1109bbdb33 shell sh /data/benchmark.sh #run queries
	adb -s 042b0f1109bbdb33 pull /data/trace.log
	mv trace.log NEWYCSB_WorkloadA_TimingA.log
done
