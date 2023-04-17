
from Table import Table
from HBase import HBase
from HFile import HFile, Row


def test_put():
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
		], 'general')
		# For address info
		hfile2 = HFile([
			Row(1, 'address:street', 1, '123 Main St'),
			Row(1, 'address:city', 1, 'New York'),
		], 'address')
		hbase.tables['test'].h_files.append(hfile)
		hbase.tables['test'].h_files.append(hfile2)
		# Act
		result = hbase.Scan('test')
		aa = 1
		
if __name__ == '__main__':
    test_put()