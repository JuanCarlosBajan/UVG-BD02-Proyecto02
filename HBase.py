
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
				])
				# For address info
				hfile2 = HFile([
						Row('1', 'address:street', '1', '123 Main St'),
						Row('1', 'address:city', '1', 'New York'),
				])
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
		
		def Get(self, table_name, row_key, columns, versions = 1):
				if table_name in self.tables.keys():
						rows = []
						for column in columns:
							cf, col = column.split(":")
							rows_found = self.tables[table_name].get(row_key, cf, col, versions)
							if rows_found != None:
								# append rows_found list contents to rows
								rows.extend(rows_found)
						return rows
				return None
		
		def Delete(self, table_name, row_key = None, column_family = None, column_name = None ,timestamp = None):
				print("Deleting row", row_key, "column family", column_family, "column name", column_name, "timestamp", timestamp, "from table", table_name,)
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