from mrjob.job import MRJob
from mrjob.step	import MRStep
import csv
from datetime import datetime
import dateutil.parser

SECONDS_FIFTEEN_MINUTES = 60 * 15

# This protocol has the logic for parsing each line of data. We separate
# the logic here away from the mapper for cleaner code.
class WeblogProtocol(object):
    def read(self, line):
		for row in csv.reader([line], delimiter=" ", quotechar='"'):
			timestamp = row[0]
			client_ip = row[2].split(':')[0]
			url = row[11].split()[1]
			return None, (timestamp, client_ip, url)

class WeblogChallenge(MRJob):

	INPUT_PROTOCOL = WeblogProtocol

	# group by client_ip
	def group_by_clientip(self, _, value):
		timestamp_str, client_ip, url = value
		yield client_ip, (timestamp_str, url)

	# sessionize
	def sessionize(self, client_ip, values):
		values = sorted(values, key=lambda x: x[0]) # sort by timestamp ASC

		start_timestamp = None
		end_timestamp = None
		unique_urls = set()
		session_seconds = 0

		for (timestamp_str, url) in values:
			timestamp = dateutil.parser.parse(timestamp_str)

			# if the current timestamp would brings us over the session
			# limit of 15 minutes, emit the previous session, and start a new one
			if start_timestamp is not None and (timestamp - start_timestamp).seconds >= SECONDS_FIFTEEN_MINUTES:
				yield (client_ip, start_timestamp.isoformat(), end_timestamp.isoformat()), (len(unique_urls), session_seconds)

				start_timestamp = None
				end_timestamp = None
				unique_urls = set()
				session_seconds = 0

			if start_timestamp is None:
				start_timestamp = timestamp
			end_timestamp = timestamp
			unique_urls.add(url)
			session_seconds = (end_timestamp - start_timestamp).seconds

		# emit any remaining session
		yield (client_ip, start_timestamp.isoformat(), end_timestamp.isoformat()), (len(unique_urls), session_seconds)

	def mapper_session_time(self, key, value):
		unique_urls, session_seconds = value
		yield 1, session_seconds

	def average_session_time(self, _, values):

		# calculate running average, because scalability on input
		count = 0
		current_avg = None

		for session_seconds in values:
			if current_avg is None:
				current_avg = session_seconds
				count = 1
			else:
				count += 1
				current_avg = ((current_avg * (count - 1)) + session_seconds) / (count * 1.0)

		yield None, {"session average" : current_avg, "num sessions" : count}

	def mapper_clientip_session_seconds(self, key, value):
		(client_ip, _, _) = key
		(_, session_seconds) = value

		yield 1, (client_ip, session_seconds)

	# If run on a production cluster, this final reducer step should be run with one reducer.
	# Note: it's tempting to just do sorted(values)[:500], but this doesn't scale well as
	# input grows. ( n log (n) for sorted vs n log (k) for heap priority queue where k = top items )
	def reducer_top500_active_clients(self, _, values):
		import heapq
		top = heapq.nlargest(500, values, key=lambda x: x[1])

		for t in top:
			yield None, t

	def steps(self):
		# Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
		# Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
		# return [MRStep(mapper=self.group_by_clientip, reducer=self.sessionize)]
	
		# Determine the average session time	
		# return [MRStep(mapper=self.group_by_clientip, reducer=self.sessionize),
		# 		MRStep(mapper=self.mapper_session_time, reducer=self.average_session_time)]

		# Find the most engaged users, ie the IPs with the longest session times
		return [MRStep(mapper=self.group_by_clientip, reducer=self.sessionize),
				MRStep(mapper=self.mapper_clientip_session_seconds, 
						reducer=self.reducer_top500_active_clients)]

if __name__ == '__main__':
	WeblogChallenge.run()
