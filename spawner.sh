#!/bin/bash

# arg0: total processes to run
# arg1: process size of transactions

usage(){
  echo "Usage: $0 STARTPOS PROCESSES SIZE"
  echo
  echo "STARTPOS: start point in database"
  echo "PROCESSES: number of independent processes to run"
  echo "SIZE: batch size"
} 

STARTPOS=$1
PROCESSES=$2
SIZE=$3

if [ -z "$STARTPOS" ] || [ -z "$PROCESSES" ] || [ -z "$SIZE" ] ; then
  usage
  exit
fi

# echo $PROCESSES

mkdir log
rm log/*

USERNAME=`ruby -ryaml -e "puts YAML::load_file('database.yml')['development']['username']"`
PASSWORD=`ruby -ryaml -e "puts YAML::load_file('database.yml')['development']['password']"`
DATABASE=`ruby -ryaml -e "puts YAML::load_file('database.yml')['development']['database']"`

mysql --user=$USERNAME --password=$PASSWORD -e "set global max_connections = 10000;"

for (( p=$STARTPOS; p<=$PROCESSES; p++ ))
do
    let START=$p*$SIZE
    echo "ruby suck_from_bart1_to_bart2.rb 0 $START $SIZE > log$p"
    `ruby suck_from_bart1_to_bart2.rb 0 $START $SIZE > log/log$p &`
done