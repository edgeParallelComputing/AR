rm scheduler.log
rm -r target
mvn clean package
cd target
cp *.jar /usr/local/storm/lib
