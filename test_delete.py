from Table import Table
from HBase import HBase
from HFile import HFile, Row


def test_delete_row():
		''' Test delete 'table_name', 'row_key', 'column_family:column_qualifier', timestamp '''
    # Arrange
		hbase = HBase()
		hbase.tables['test'] = Table('users', ['general', 'address'])
		hbase.tables['test'].add_column('general', 'name')
		hbase.tables['test'].add_column('general', 'age')
		hbase.tables['test'].add_column('address', 'street')
		hbase.tables['test'].add_column('address', 'city')
		
		hfile = HFile([
			Row(1, 'general:name', 1, 'John'),
			Row(1, 'general:age', 1, 20),
		])
		# For address info
		hfile2 = HFile([
			Row(1, 'address:street', 1, '123 Main St'),
			Row(1, 'address:city', 1, 'New York'),
		])
		hbase.tables['test'].h_files.append(hfile)
		hbase.tables['test'].h_files.append(hfile2)

		# Act
		deleted = hbase.Delete('test', 1, 'general','name', 1)
		# Assert
		assert deleted == 1
		assert hbase.tables['test'].h_files[0].rows[0].enabled == False
		assert hbase.tables['test'].h_files[0].rows[1].enabled == True