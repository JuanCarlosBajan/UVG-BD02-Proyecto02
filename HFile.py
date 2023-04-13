
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
	
		
		
		
						