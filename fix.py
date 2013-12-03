"""
Run this script every few minutes via cron to make sure that sphinx behaves properly.

For some reason newznab's sphinx.php does not reliably set the last/next datetimes after merge/rebuilds.
sphinx.php sends the correct SQL statement to db.php, but it just doesn't do anything!  This leads to rebuild/merge 
running continuously.

I got tired of trying to figure it out (lol, PHP) so wrote this script in an hour and now don't worry about it.

Note that if you run this too quickly, it could set the next merge or rebuild date too quickly.  For example:

1. Merge time is at 3:00 AM.
2. Update script (like nix_scripts/newsnab*.sh) is set to run every 5 minutes and the next run turns out to be
   at 3:03 AM.
3. Your cronjob that runs this script runs this script at 3:02 AM which sets the next merge date at 3:00 AM tomorrow.
4. The update script runs as scheduled at 3:03 AM sees that the next merge date is tomorrow and never runs the sphinx
   merge.

To avoid this, just set the cronjob for this script at a longer interval than however often your newznab update
script runs.
"""
import datetime
import os
import shlex
import sys
import time

import oursql
from dateutil.relativedelta import relativedelta, SU, MO, TU, WE, TH, FR, SA


def get_db_config(config_file):
	"""Parse the db connection details out of config.php"""
	assert os.path.exists(config_file), "{} does not exist".format(config_file)
	with open(config_file, "r") as f:
		config = f.read()

	db_config = {}
	for line in config.splitlines():
		split = shlex.split(line)
		if not split:
			continue
		if 'DB_HOST' in split[0]:
			db_config['host'] = split[1][:-2]
		elif 'DB_PORT' in split[0]:
			db_config['port'] = int(split[1][:-2])
		elif 'DB_USER' in split[0]:
			db_config['user'] = split[1][:-2]
		elif 'DB_PASSWORD' in split[0]:
			db_config['passwd'] = split[1][:-2]
		elif 'DB_NAME' in split[0]:
			db_config['db'] = split[1][:-2]

		if len(db_config) == 5:
			break

	assert len(db_config) == 5, "{} is an invalid config file".format(config_file)

	return db_config


def get_sphinx_rows(cursor):
	"""Get active indexes"""
	cursor.execute("SELECT * FROM sphinx WHERE maxID > 0")
	return cursor.fetchall()


def get_sphinx_config(cursor):
	"""Get sphinx config from db"""
	# Get merge freq
	cursor.execute("SELECT value FROM site WHERE setting = 'sphinxmergefreq'")
	merge_freq = cursor.fetchall()[0]['value']

	# Get merge freq count
	cursor.execute("SELECT value FROM site WHERE setting = 'sphinxmergefreq_count'")
	merge_count = cursor.fetchall()[0]['value']

	# Get rebuild day
	cursor.execute("SELECT value FROM site WHERE setting = 'sphinxrebuildfreq_day'")
	rebuild_day = cursor.fetchall()[0]['value']

	# Get rebuid time
	cursor.execute("SELECT value FROM site WHERE setting = 'sphinxrebuildfreq'")
	rebuild_time = cursor.fetchall()[0]['value']

	ret = {'sphinxmergefreq': merge_freq,
	       'sphinxmergefreq_count': merge_count,
	       'sphinxrebuildfreq_day': rebuild_day,
	       'sphinxrebuildfreq': rebuild_time}
	return ret


def next_rebuild_date(sphinx_config, cursor, id_):
	"""
	Sets the next rebuild day in the db.

	:param sphinx_config: A config dict as returned by `get_sphinx_config`
	:type sphinx_config: dict
	:param cursor: A DB-API 2.0 cursor using `qmark` style parameterization
	:type cursor: cursor
	:param id_: The row ID of the index to update
	:type id_: int
	"""
	day = get_day(sphinx_config['sphinxrebuildfreq_day'])
	if not day:
		# rebuilds are disabled
		return

	sql = "UPDATE sphinx SET lastrebuilddate = NOW(), nextrebuilddate = ? WHERE ID = ?"
	now = get_mysql_now(cursor)

	# convert the string like "0330" into an hour and minute
	config_dt = datetime.datetime.strptime(sphinx_config['sphinxrebuildfreq'], '%H%M')

	# Create a datetime for today set to the hour and minute configured for sphinx
	base_dt = datetime.datetime(now.year, now.month, now.day, config_dt.hour, config_dt.minute, 0)

	# Get next weekday configured to run.  Dateutil takes care of weekdays
	# crossing year/month/leap/whatever boundaries
	next = base_dt + relativedelta(weekday=day)

	# Update the db
	cursor.execute(sql, (next, id_))


def get_day(day_name):
	"""Gets the dateutil day for the corresponding weekday name"""
	if day_name == 'Sunday':
		return SU
	if day_name == 'Monday':
		return MO
	if day_name == 'Tuesday':
		return TU
	if day_name == 'Wednesday':
		return WE
	if day_name == 'Thursday':
		return TH
	if day_name == 'Friday':
		return FR
	if day_name == 'Saturday':
		return SA


def next_merge_date_sql(sphinx_config, cursor, id_):
	"""
	Sets the next merge day in the db.

	:param sphinx_config: A config dict as returned by `get_sphinx_config`
	:type sphinx_config: dict
	:param cursor: A DB-API 2.0 cursor using `qmark` style parameterization
	:type cursor: cursor
	:param id_: The row ID of the index to update
	:type id_: int
	"""
	sql = "UPDATE sphinx SET lastmergedate = NOW(), nextmergedate = ? WHERE ID = ?"
	now = get_mysql_now(cursor)

	# convert the string like "0330" into an hour and minute
	config_dt = datetime.datetime.strptime(sphinx_config['sphinxmergefreq'], '%H%M')

	# Create a datetime for today at the configured hour and minute
	today_merge = datetime.datetime(now.year, now.month, now.day, config_dt.hour, config_dt.minute, 0)


	if today_merge > now:
		# If we haven't go to the configured hour and minute on today, use that datetime
		next = today_merge
	else:
		# If we've passed the hour/minute for today, use that hour/minute tomorrow
		next = today_merge + relativedelta(days=+1)

	cursor.execute(sql, (next, id_))


def get_mysql_now(cursor):
	"""
	Gets the current datetime from the database that `cursor` is connected to.

	Use this instead of something `like datetime.datetime.now()` so that we no the current datetime on the database
	instead of the machine this script is running on.

	:param cursor: A DB-API 2.0 cursor using `qmark` style parameterization
	:returns: The datetime on the database server
	:rtype: datetime.datetime
	"""
	cursor.execute("SELECT NOW()")
	return cursor.fetchall()[0]['NOW()']


if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "Please provide the path to your newznab config.php file like so: \n\t{} path_to_config".format(
			sys.argv[0])
		sys.exit(2)
	NN_CONFIG = sys.argv[1]

	# Try up to 10 times on event of database connectivity errors
	for i in range(10):
		try:
			with oursql.connect(**get_db_config(NN_CONFIG)).cursor(oursql.DictCursor) as cursor:
				sphinx_config = get_sphinx_config(cursor)

				# Loop through the enabled sphinx indexes
				for row in get_sphinx_rows(cursor):
					if row['nextmergedate'] < datetime.datetime.now():
						print "{} next merge date of {} has past.  Updating.".format(row['name'], row['nextmergedate'])
						next_merge_date_sql(sphinx_config, cursor, row['ID'])

					if row['nextrebuilddate'] < datetime.datetime.now():
						print "{} next rebuild date of {} has past.  Updating.".format(row['name'],
						                                                               row['nextrebuilddate'])
						next_rebuild_date(sphinx_config, cursor, row['ID'])

			break
		except oursql.OperationalError as e:
			if 'server has gone away' in str(e):
				# Let's wait a bit for server to come back
				time.sleep(10)
