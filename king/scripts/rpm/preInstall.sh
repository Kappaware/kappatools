if [ -f /etc/king/setenv.sh ]; then
 	mv /etc/king/setenv.sh /etc/king/setenv.sh.bck
fi

/usr/bin/getent passwd king || /usr/sbin/useradd -r -d /opt/king -s /sbin/nologin king

if [ ! -d /var/log/king ]; then
	mkdir /var/log/king
	chown king:king /var/log/king
fi
