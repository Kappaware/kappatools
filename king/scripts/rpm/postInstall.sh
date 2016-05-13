
if [ -x /bin/systemctl ]; then
	cp /opt/king/king.service /usr/lib/systemd/system/king.service
	systemctl daemon-reload
	systemctl enable king
else 
	if [ -x /sbin/chkconfig ]; then
		cp /opt/king/init.sh /etc/init.d/king
		/sbin/chkconfig --add king
	elif [ -x /usr/lib/lsb/install_initd ]; then
		p /opt/king/init.sh /etc/init.d/king
		/usr/lib/lsb/install_initd /etc/init.d/king
	else
		cp /opt/king/init.sh /etc/init.d/king
		for i in 2 3 4 5; do
			ln -sf /etc/init.d/king /etc/rc.d/rc${i}.d/S11king
		done
		for i in 0 1 6; do
	    	ln -sf /etc/init.d/king /etc/rc.d/rc${i}.d/K88king
		done
	fi
fi
