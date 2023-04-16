
from HBase import HBase
from HFile import HFile, Row
from Table import Table


def test_count():
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
                Row(2, 'address:street', 1, '5th Ave'),
		], 'address')
		hbase.tables['test'].h_files.append(hfile)
		hbase.tables['test'].h_files.append(hfile2)
		# Act and Assert
		assert hbase.Count('test') == 5
		

if __name__ == '__main__':
    test_count()

