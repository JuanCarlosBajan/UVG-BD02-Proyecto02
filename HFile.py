
import time

class HFile:
		''' Class for handling files '''
		''' An HFile has an array of columns and an array of rows '''

		
		def __init__(self, name, columns, rows):
			self.name = name
			self.columns = columns
			self.rows = rows

		
		# Getters
		def get_name(self):
			return self.name
		def get_columns(self):
			return self.columns	
		def get_rows(self):
			return self.rows	
		

		# HBase methods
		def create_row(self, key, column, value):
			''' Creates a row with the given key, column and value the tiemestamp is set to current time '''
			''' Returns the row created '''
			row = Row(key, column, time.time(), value, True)
			self.rows.append(row)
			return row


class Row:
	''' Class for handling rows '''
	''' A row has a key, column name, timestamp, value and a boolean indicating if it is enabled '''

	def __init__(self, key, column, timestamp, value, enabled):
		self.key = key
		self.column = column
		self.timestamp = timestamp
		self.value = value
		self.enabled = enabled
	
	# Method for disabling a row
	def disable(self):
		self.enabled = False
						