from HBase import HBase
from HFile import HFile, Row
from Table import Table


def test_truncate():
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
		hbase.Truncate('test')
		# Assert
		assert len(hbase.tables['test'].h_files) == 0
		assert len(hbase.tables['test'].h_files) == 0
		assert hbase.tables['test'].is_enabled() == True