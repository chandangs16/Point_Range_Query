#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    curs=openconnection.cursor()

    range_name = "RangeRatingsPart"
    rrobin_name="RoundRobinRatingsPart"
    f=open('RangeQueryOut.txt', 'w')

    curs.execute("Select PartitionNum from RangeRatingsMetadata where "+str(ratingMinValue)+"<=minrating or "+ str(ratingMaxValue)+">=maxrating")
    num_range_parts=curs.fetchall()
    print (num_range_parts)

    for each in num_range_parts:
        tableName=range_name+str(each[0])
        curs.execute("Select * from "+tableName+" where Rating>="+str(ratingMinValue)+" AND Rating <="+str(ratingMaxValue))
        rows = curs.fetchall()
        print(rows)
        for each in rows:
            f.write(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]) + "\n")
        #Copy(Select * From foo) To '/tmp/test.csv' With CSV DELIMITER ',';
    print("range query range part done")


    curs.execute("Select Partitionnum from RoundRobinRatingsMetadata")
    num_rrobin_parts = curs.fetchall()
    print("round robin parts:")
    print (num_rrobin_parts)

    for each in range(0,num_rrobin_parts[0][0]):
        tableName=rrobin_name+str(each)
        curs.execute("Select * from "+tableName+" where Rating>="+str(ratingMinValue)+" AND Rating <="+str(ratingMaxValue))
        r=curs.fetchall()
        for each in r:
            f.write(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]) + "\n")


    print("range query rrobin part done")
    #cur.close()
    f.close()



    #pass #Remove this once you are done with implementation

def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.

    curs = openconnection.cursor()
    # curs.execute("DROP TABLE IF EXISTS " + ratingstablename)
    range_name = "RangeRatingsPart"
    rrobin_name = "RoundRobinRatingsPart"

    f = open('PointQueryOut.txt', 'w')

    curs.execute("Select PartitionNum from RangeRatingsMetadata where " + str(ratingValue) + ">=minrating and " + str(ratingValue) + "<=maxrating")
    num_range_parts = curs.fetchall()
    print (num_range_parts)

    for each in num_range_parts:
        tableName = range_name + str(each[0])
        curs.execute("Select * from " + tableName + " where Rating ="  +str(ratingValue))
        rows = curs.fetchall()
        for each in rows:
            f.write(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]) + "\n")
            # Copy(Select * From foo) To '/tmp/test.csv' With CSV DELIMITER ',';

    print("point query range part done")

    curs.execute("Select Partitionnum from RoundRobinRatingsMetadata")
    num_rrobin_parts = curs.fetchall()
    print("point rrobin")
    print (num_rrobin_parts)

    for each in range(0,num_rrobin_parts[0][0]):
        tableName=rrobin_name+str(each)
        curs.execute("Select * from " + tableName + " where Rating =  "+ str(ratingValue))
        r = curs.fetchall()
        for each in r:
            f.write(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]) + "\n")


    print("point query rrobin part done")
    #cur.close()
    f.close()


