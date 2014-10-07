#! /bin/sh

timeout=3

# check number of arguments
if [ $# -lt 1 ]
then
	echo "Give executable [folder]"
	exit 1
fi

# configuration file
conf="conf"
if [ ! -f $conf ]
then
	echo "Configuration not found"
	exit 1
fi

# compile executable
exe=$1
if [ $exe != "hj" -a $exe != "tj2" -a $exe != "tj3" -a $exe != "tj4" -a $exe != "tj" ]
then
	echo "Give executable: hj/tj2/tj3/tj4"
	exit 1
fi
make $exe
sleep $timeout

# check for feed file or random
#args="10 10"
#if [ "$2" != "" -a -d $2 ]
#then
#	if [ ! -f $2/R ]
#	then
#		echo "Key file of R not found"
#		exit 1
#	fi
#	if [ ! -f $2/S ]
#	then
#		echo "Key file of S not found"
#		exit 1
#	fi
#	args=""
#elif [ "$2" != "" ]
#then
#	v=`expr $2 + 0 2> /dev/null`
#	if [ $? -ne 0 ]
#	then
#		echo "Neither valid directory nor valid number"
#		exit 1
#	fi
#	args="$v $v"
#fi

# read hosts
hosts=""
while read line
do
	host=`echo $line | cut -f1 -d' '`
	hosts="$hosts $host"
done < $conf
echo $hosts

# rewrite configuration
rm $conf
touch $conf
for host in $hosts
do
	port=`./random 20001 49999`
	echo $host $port $exe >> $conf
done
sleep $timeout

# run track join
for host in $hosts
do
	sleep $timeout
	echo "Running on $host"
	ssh $host "/home/xinlu/tj/$exe" 
done

# run feed file
#if [ "$2" != "" -a -d $2 ]
#then
#	sleep $timeout
#	sleep $timeout
#	./feed $2/R $2/S $conf $3
#fi

wait
