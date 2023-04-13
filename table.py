


class Table:
		
		# Static variable key
		key = 0

		def __init__(self, name, groups):
				self.name = name
				self.family_columns = {}
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

		def change_name(self, name):
				if not self.enabled:
						return False
				self.name = name

		