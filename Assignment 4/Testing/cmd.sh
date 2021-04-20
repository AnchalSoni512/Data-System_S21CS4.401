# output file = ri.txt_si.txt_join
/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r1.txt s1.txt sort 2
sort -u r1.txt_s1.txt_join.txt > output.txt
comm -3 out1.txt output.txt

/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r2.txt s2.txt sort 2
sort -u r2.txt_s2.txt_join.txt > output.txt
comm -3 out2.txt output.txt

/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r3.txt s3.txt sort 4
sort -u r3.txt_s3.txt_join.txt > output.txt
comm -3 out3.txt output.txt

/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r4.txt s4.txt sort 5
sort -u r4.txt_s4.txt_join.txt > output.txt
comm -3 out4.txt output.txt

/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r1.txt s1.txt hash 2
sort -u r1.txt_s1.txt_join.txt > output.txt
comm -3 out1.txt output.txt

/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r2.txt s2.txt hash 2
sort -u r2.txt_s2.txt_join.txt > output.txt
comm -3 out2.txt output.txt

/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r3.txt s3.txt hash 4
sort -u r3.txt_s3.txt_join.txt > output.txt
comm -3 out3.txt output.txt

/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r4.txt s4.txt hash 5
sort -u r4.txt_s4.txt_join.txt > output.txt
comm -3 out4.txt output.txt

#Error handling
/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r1.txt s1.txt hash 0
/usr/bin/time -f "memory usage = %M" bash 2020201099.sh r1.txt s1.txt sort 1
