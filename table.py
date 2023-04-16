


class Table:

		def __init__(self, name, groups):
				self.name = name
				self.family_columns = {}
				self.h_files = []
				self.enabled = True
				for fam in groups:
						self.family_columns[fam] = ['nombre', 'apellido']

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
				if family not in self.family_columns.keys:
						return False
				if column in self.family_columns[family]:
						return False
				self.family_columns[family].append(column)
				return True

		def change_name(self, name):
				if not self.enabled:
						return False
				self.name = name

		def put():
			pass

		