
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
		

		def get(self, key, column_family, column, version = 1):
			# Order rows by timestamp
			self.rows.sort(key=lambda x: x.timestamp, reverse=False)
			rows_found = []
			counter = 0
			for row in self.rows:
				col_key = column_family + ":" + column
				if row.key == key and row.column == col_key and row.enabled == True:
					counter += 1
					if counter == version:
						rows_found.append(row)
						break
			return rows_found
		

		def delete(self, key, column_family = None, column = None, timestamp = None):
			''' Deletes a row with the given key, column and timestamp '''
			''' Returns true if the row was deleted '''
			rows_deleted = 0
			for row in self.rows:
				# If all parameters are None, delete all rows with the given key
				if column_family == None and column == None and timestamp == None:
					if row.key == key and row.enabled == True:
						row.disable()
						rows_deleted += 1
				# If only column family is given, delete all rows with the given key and column family
				if column_family != None and column == None and timestamp == None:
					if row.key == key and column_family in row.column and row.enabled == True:
						row.disable()
						rows_deleted += 1
				# If only column is given, delete all rows with the given key and column
				if column_family != None and column != None and timestamp == None:
					col_key = column_family + ":" + column
					if row.key == key and row.column == col_key and row.enabled == True:
						row.disable()
						rows_deleted += 1
				if column_family != None and column != None and timestamp != None:
					col_key = column_family + ":" + column
					if row.key == key and row.column == col_key and row.timestamp == timestamp and row.enabled == True:
						row.disable()
						rows_deleted += 1
			return rows_deleted


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
						