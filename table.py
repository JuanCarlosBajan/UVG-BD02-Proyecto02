


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
			

		def put():
			pass

		