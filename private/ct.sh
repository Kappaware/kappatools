
MYDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PGPASSWORD=Netbios;
/Library/PostgreSQL/9.3/bin/psql --host localhost --username postgres --dbname kappatools --file=${MYDIR}/ct.sql



