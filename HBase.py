
from Table import Table
from HFile import HFile, Row
import numpy as np
import csv


class HBase:

		def __init__(self):
				self.tables = {}


		def Load_DataSet(self):
			data = []

			with open('books.csv', 'r', encoding='utf-8') as csvfile:
				reader = csv.reader(csvfile)
				for row in reader:
					if len(row) == 12:
						data.append(row)

			data = np.array(data)
			data = data[~np.all(data == '', axis=1)]
			data = data[1:]

			
			self.tables['initial'] = Table('books', ['general', 'identifiers', 'reviews', 'publication'])
			self.tables['initial'].add_column('general', 'bookID')
			self.tables['initial'].add_column('general', 'title')
			self.tables['initial'].add_column('general', 'authors')
			self.tables['initial'].add_column('general', 'languageCode')
			self.tables['initial'].add_column('general', 'noOfPages')

			self.tables['initial'].add_column('identifiers', 'isbn')
			self.tables['initial'].add_column('identifiers', 'isbn13')

			self.tables['initial'].add_column('reviews', 'averageRating')
			self.tables['initial'].add_column('reviews', 'ratingsCount')
			self.tables['initial'].add_column('reviews', 'textReviewsCount')

			self.tables['initial'].add_column('publication', 'publisher')
			self.tables['initial'].add_column('publication', 'publicationDate')

			# bookID,title,authors,average_rating,isbn,isbn13,language_code,  num_pages,ratings_count,text_reviews_count,publication_date,publisher

			count_general = 1
			for row in data:
				self.Put('initial', str(count_general), 'general' , 'bookID', row[0])
				self.Put('initial', str(count_general), 'general' , 'title', row[1])
				self.Put('initial', str(count_general), 'general' , 'authors', row[2])
				self.Put('initial', str(count_general), 'general' , 'languageCode', row[6])
				self.Put('initial', str(count_general), 'general' , 'noOfPages', row[7])
				count_general += 1

			count_identifiers = 1
			for row in data:
				self.Put('initial', str(count_identifiers), 'identifiers' , 'isbn', row[4])
				self.Put('initial', str(count_identifiers), 'identifiers' , 'isbn13', row[5])
				count_identifiers += 1

			count_reviews = 1
			for row in data:
				self.Put('initial', str(count_reviews), 'reviews' , 'averageRating', row[3])
				self.Put('initial', str(count_reviews), 'reviews' , 'ratingsCount', row[8])
				self.Put('initial', str(count_reviews), 'reviews' , 'textReviewsCount', row[9])
				count_reviews += 1

			count_publication = 1
			for row in data:
				self.Put('initial', str(count_publication), 'publication' , 'publisher', row[11])
				self.Put('initial', str(count_publication), 'publication' , 'publicationDate', row[10])
				count_publication += 1

			print('Data Cargada Exitosamente!!!')
			


			



	
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
						self.tables[new_name] = old_table
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
		
		def Describe(self, table_name):
			if table_name not in self.tables.keys():
				return False
			self.tables[table_name].describe()
			return True
		

if __name__ == '__main__':
	hb = HBase()
	hb.Load_DataSet()