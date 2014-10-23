#! /bin/sh

timeout=3
exe="join"

# check number of arguments
if [ $# -lt 1 ]
then
	echo "Give join algorithm code"
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
algo=$1
if [ $algo != "hj" -a $algo != "tj2" -a $algo != "tj3" -a $algo != "tj4" -a $algo != "test" ]
then
	echo "Give join algorithm code: hj/tj2/tj3/tj4/test"
	exit 1
fi
make
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
	ssh $host "$PWD/$exe $algo" &
done

# run feed file
#if [ "$2" != "" -a -d $2 ]
#then
#	sleep $timeout
#	sleep $timeout
#	./feed $2/R $2/S $conf $3
#fi

wait
