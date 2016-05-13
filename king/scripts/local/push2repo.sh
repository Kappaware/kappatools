#!/bin/bash

# To push our jar in some remote repositories, defined as a list
# Example:
# repositories="server1.broadsoftware.com:/var/www/repos/misc server2.broadsoftware.com:/var/www/repos/misc"

rpm="build/distributions/king-*.noarch.rpm"


rpmb=$(basename $rpm)

for r in ${repositories}; do
	echo "Will push $rpmb to $r/$rpmb"
	scp $rpm $r/$rpmb 
done
