
from Table import Table
from HFile import HFile, Row


class HBase:

		def __init__(self):
				self.tables = {}
	
		def Create_Test_Table(self):
				self.tables['test'] = Table('users', ['general', 'address'])
				self.tables['test'].add_column('general', 'name')
				self.tables['test'].add_column('general', 'age')
				self.tables['test'].add_column('address', 'street')
				self.tables['test'].add_column('address', 'city')
				hfile = HFile([
				Row('1', 'general:name', '1', 'John'),
				Row('1', 'general:age', '1', 20),
				], 'general')
				# For address info
				hfile2 = HFile([
						Row('1', 'address:street', '1', '123 Main St'),
						Row('1', 'address:city', '1', 'New York'),
				], 'address')
				self.tables['test'].h_files.append(hfile)
				self.tables['test'].h_files.append(hfile2)

		def Create_Test_Table2(self):
			self.tables['test'] = Table('users', ['general', 'address'])
			self.tables['test'].add_column('general', 'name')
			self.tables['test'].add_column('general', 'age')
			self.tables['test'].add_column('address', 'street')
			self.tables['test'].add_column('address', 'city')

			hfile = HFile([
				Row(1, 'general:name', 1, 'John'),
				Row(1, 'general:age', 1, 20),
				Row(2, 'general:name', 1, 'Joshua'),
				Row(2, 'general:age', 1, 25),
				Row(3, 'general:name', 1, 'Sofia'),
				Row(3, 'general:age', 1, 22),
				Row(4, 'general:name', 1, 'Rose'),
				Row(4, 'general:age', 1, 23),
				Row(5, 'general:name', 1, 'Lily'),
				Row(5, 'general:age', 1, 24),
			], 'general')
			# For address info
			hfile2 = HFile([
				Row(1, 'address:street', 1, '123 Main St'),
				Row(1, 'address:city', 1, 'New York'),
				Row(2, 'address:street', 1, '456 Main St'),
				Row(2, 'address:city', 1, 'San Francisco'),
				Row(3, 'address:street', 1, '789 Main St'),
				Row(3, 'address:city', 1, 'Miami'),
				Row(4, 'address:street', 1, '101 Main St'),
				Row(4, 'address:city', 1, 'Los Angeles'),
				Row(5, 'address:street', 1, '102 Main St'),
				Row(5, 'address:city', 1, 'Chicago'),
			], 'address')
			self.tables['test'].h_files.append(hfile)
			self.tables['test'].h_files.append(hfile2)

		def Create(self, name, family_columns):
				if name not in self.tables.keys():
						table = Table(name, family_columns)
						self.tables[name] = table
						return True
				return False
		
		def List(self):
				return self.tables.keys()
		
		def Disable(self, name):
				if name in self.tables.keys():
						self.tables[name].disable()
						return True
				return False
		
		def Enable(self, name):
				if name in self.tables.keys():
						self.tables[name].enable()
						return True
				return False

		def Is_Enabled(self, name):
				if name in self.tables.keys():
						return self.tables[name].is_enabled()
				return False
		
		def Alter_Table_Name(self,table, new_name):
				found_old = table in self.tables.keys()
				found_new = new_name in self.tables.keys()
				
				if found_old and not found_new:
						old_table = self.tables[table]
						del self.tables[table]
						old_table.change_name(new_name)
						self.tables[found_new] = old_table
						return True
				
				return False
		
		def Alter_Table_Add(self, name, column_family):
				if name in self.tables.keys:
						return self.tables[name].add_family(column_family)
				return False
		
		def Alter_Table_Delete(self, name, column_family):
				if name in self.tables.keys:
						return self.tables[name].delete_family(column_family)
				return False
		
		def Alter_Table_Add_Column(self, name, column_family, column):
				if name in self.tables.keys:
						return self.tables[name].add_column(column_family, column)
				return False
		
		def Delete_Table(self, name):
				if name in self.tables.keys and not self.tables[name].is_enabled():
						del self.tables[name]
						return True
				return False
		
		def Delete_Table_All(self):
				self.tables = {}
				return True
		
		def Get(self, table_name, row_key, columns, version = 1):
				if table_name in self.tables.keys():
						rows = []
						for column in columns:
							cf, col = column.split(":")
							rows_found = self.tables[table_name].get(row_key, cf, col, version)
							if rows_found != None:
								# append rows_found list contents to rows
								rows.extend(rows_found)
						return rows
				return None
		
		def Put(self, table_name, row_key, column_family, column_name, value, timestamp = None):
				if table_name in self.tables.keys():
						return self.tables[table_name].put(table_name, row_key, column_family, column_name, value, timestamp)
				return False
		
		def Delete(self, table_name, row_key = None, column_family = None, column_name = None ,timestamp = None):
				if table_name in self.tables.keys():
						return self.tables[table_name].delete(row_key, column_family, column_name, timestamp)
				return 0
		
		def Truncate(self, table_name):
				if table_name in self.tables.keys():
						self.Disable(table_name)
						self.tables[table_name].truncate()
						self.Enable(table_name)
						return True
				return False
		
		def Count(self, table_name):
				if table_name in self.tables.keys():
						return self.tables[table_name].count()
				return False
		
		def Scan(self, table_name, row_start = None, row_stop = None, limit = None):
			if table_name in self.tables.keys():
				return self.tables[table_name].scan(row_start, row_stop, limit)
			return None