
import time

class HFile:
		''' Class for handling files '''
		''' An HFile has an array of columns and an array of rows '''
		rows = []
		
		def __init__(self, rows, column_family):
			self.rows = rows
			self.column_family = column_family

		
		def get_rows(self):
			return self.rows	
		

		# HBase methods
		def create_row(self, key, column, value):
			''' Creates a row with the given key, column and value the tiemestamp is set to current time '''
			''' Returns the row created '''
			row = Row(key, column, time.time(), value, True)
			self.rows.append(row)
			return row
		

		def get(self, key, column_family, column, versions = 1):
			# Order rows by timestamp
			self.rows.sort(key=lambda x: x.timestamp, reverse=True)
			rows_found = []
			for row in self.rows:
				col_key = column_family + ":" + column
				if row.key == key and row.column == col_key and row.enabled == True and len(rows_found) <= versions:
					rows_found.append(row)
			return rows_found


class Row:
	''' Class for handling rows '''
	''' A row has a key, column name, timestamp, value and a boolean indicating if it is enabled '''

	def __init__(self, key, column, timestamp, value, enabled = True):
		self.key = key
		self.column = column	# ColumnFamily:Column
		self.timestamp = timestamp
		self.value = value
		self.enabled = enabled
	
	# Method for disabling a row
	def disable(self):
		self.enabled = False
						