
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

		def put(self, table_name, row_key, column_family, column_name, value):
			if not self.enabled:
				return False
			if table_name != self.name:
				return False
			if column_family not in self.family_columns.keys():
				return False
			if column_name not in self.family_columns[column_family]:
				return False
			
			if not self.h_files:
				row_t = Row(row_key, column_name, time.time(), value)
				h_file_t = HFile(row_t, column_family)
				self.h_file_t.create_row(h_file_t)
			else:
				for hf in self.h_files:
					if column_family == hf.column_family:
						hf.add_row(row_key, column_name, time.time(), value)
						return True
					else:
						return False
				

		def get(self, row_key, column_family, column, versions = 1):
				if not self.enabled:
						return None
				if column_family not in self.family_columns.keys():
						return None
				if column not in self.family_columns[column_family]:
						return None
				for h_file in self.h_files:
						rows = h_file.get(row_key, column_family, column, versions)
						if len(rows) != 0:
								return rows
				return None
		