
from HFile import HFile
from HFile import Row

import time

class Table:

		def __init__(self, name, groups):
				self.name = name
				self.family_columns = {}
				self.h_files = []
				self.enabled = True
				for fam in groups:
						self.family_columns[fam] = []

		def disable(self):
				self.enabled = False
		
		def enable(self):
				self.enabled = True

		def is_enabled(self):
				return self.enabled

		def add_family(self, name):
				if not self.enabled:
						return False
				if name in self.family_columns.keys:
						return False
				self.family_columns[name] = []
				return True
		
		def delete_family(self, name):
				if not self.enabled:
						return False
				
				if name not in self.family_columns.keys:
						return False
				
				del self.family_columns[name]
				return True
		
		def add_column(self, family, column):
				if not self.enabled:
						return False
				if family not in self.family_columns.keys():
						return False
				if column in self.family_columns[family]:
						return False
				self.family_columns[family].append(column)
				return True

		def change_name(self, name):
				if not self.enabled:
						return False
				self.name = name

		def put(self, table_name, row_key, column_family, column_name, value, timestamp = None):
			
			if not self.enabled:
				return False
			if column_family not in self.family_columns.keys():
				return False
			if column_name not in self.family_columns[column_family]:
				return False
			
			if not timestamp:
				timestamp = time.time()
			
			if not self.h_files:
				row_t = Row(row_key, f'{column_family}:{column_name}', timestamp, value)
				h_file_t = HFile([row_t], column_family)
				self.h_files = [h_file_t]
				return True
			else:
				for hf in self.h_files:
					if column_family == hf.column_family:
						hf.create_row(row_key, f'{column_family}:{column_name}', value, timestamp)
						return True
					else:
						return False
				

		def get(self, row_key, column_family, column, version = 1):
				if not self.enabled:
						return None
				if column_family not in self.family_columns.keys():
						return None
				if column not in self.family_columns[column_family]:
						return None
				for h_file in self.h_files:
						rows = h_file.get(row_key, column_family, column, version)
						if len(rows) != 0:
								return rows
				return None
		
		def delete(self, row_key, column_family = None, column = None, timestamp = None):
				deleted_rows = 0
				if not self.enabled:
						return deleted_rows
				if column_family != None and column_family not in self.family_columns.keys():
						return deleted_rows
				if column != None and column not in self.family_columns[column_family]:
						return deleted_rows
				for h_file in self.h_files:
						deleted_rows += h_file.delete(row_key, column_family, column, timestamp)
				return deleted_rows
			
		def truncate(self):
				self.h_files = []
				return True
		
		def count(self):
				count = 0
				for h_file in self.h_files:
						count += len(h_file.rows)
				return count
		
		def scan(self, start_row = None, end_row = None, limit = None):
				if not self.enabled:
						return False
				
				rows = []
				if not start_row and not end_row:
					count = 0
					for h_file in self.h_files:
							for row in h_file.rows:
									if limit and count == limit:
										break
									rows.append(row)
									count += 1
				
				if start_row and end_row:
					for h_file in self.h_files:
							for row in h_file.rows:
									if row.key >= start_row and row.key < end_row:
											rows.append(row)
				return rows
		
		def describe(self):
			enabled = "ENABLED" if self.enabled else "DISABLED"
			print("Table " + self.name + " is " + enabled)
			print(self.name)
			print("COLUMN FAMILIES DESCRIPTION")
			for column_family in self.family_columns.keys():
				print("{NAME => '" + column_family + "' VERSIONS => '1'}")
			print(str(len(self.family_columns.keys())) + " row(s)")
