if [ -x /bin/systemctl ]; then
	/bin/systemctl stop king.service
	rm -f /usr/lib/systemd/system/king.service
	systemctl daemon-reload
else
	if [ $1 = 0 ]; then
		/etc/init.d/king stop  > /dev/null 2>&1
		if [ -x /sbin/chkconfig ]; then
			/sbin/chkconfig --del king
		elif [ -x /usr/lib/lsb/remove_initd ]; then
			/usr/lib/lsb/install_initd /etc/init.d/king
		else
	    	rm -f /etc/rc.d/rc?.d/???king
	    fi
	    rm -f /etc/init.d/king
	fi
fi
	
if [ -f /etc/king/setenv.sh ]; then
 	cp /etc/king/setenv.sh /etc/king/setenv.sh.bck
fi
